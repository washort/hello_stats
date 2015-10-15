#!/usr/bin/env python
"""Determine how often it occurs that 2 people are attempting to be in a room
together and actually succeed in communicating. Write that data to an S3
bucket so a dashboard can pull it out.

Usage::

    ES_URL=... ES_USERNAME=... ES_PASSWORD=... python state_histogram.py

Specifically, for each set of overlapping join-leave spans in a room, what is
the furthest state the link-clicker reaches (since we can't tell how far the
built-in client gets, and sendrecv implies at least one party has
bidirectional flow)? We emit a histogram of the answer to that question.

Idealized state sequences for the 2 different types of clients::

    Unregistered/Registered:
        action = join refresh* leave
        state = <none>

    Link-clicker:
        action = join (status / refresh)+ leave
        state = waiting starting receiving sending? sendrecv  # These can come out of order due to varying latencies in transports.

    Iff action=status, there's a state.

"""
from collections import OrderedDict
from datetime import datetime, timedelta, date
from itertools import groupby
import json
from os import environ
from os.path import dirname, join
import cPickle as pickle

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from pyelasticsearch import ElasticSearch


# Update this, and the next update will wipe out all saved data and start from
# scratch. A good reason to do this is if you improve the accuracy of the
# computations.
VERSION = 7

# The first date for which data for this metric was available in ES is 4/30.
# Before this, the userType field is missing. I suspect the data is wrong on
# 4/30, as the sendrecv rate is ridiculously low, so I'm starting at 5/1.
BEGINNING_OF_TIME = date(2015, 8, 20)

# Number of seconds without network activity to allow before assuming a room
# participant is timed out. When this many seconds goes by, they time out.
TIMEOUT_SECS = 60 * 5
TIMEOUT_DURATION = timedelta(0, TIMEOUT_SECS)


class StateCounter(object):
    """A histogram of link-clicker states"""

    def __init__(self, buckets=()):
        """If you want a bucket to be guaranteed to show up, pass it in as one
        of ``buckets``. Otherwise, I'll make them dynamically."""
        self.total = 0
        self.d = OrderedDict((b, 0) for b in buckets)

    def incr(self, state):
        self.d[state] = self.d.get(state, 0) + 1
        self.total += 1

    def histogram(self, stars=100):
        """Return an ASCII-art bar chart for debugging and exploration."""
        ret = []
        STARS = 100
        for state, count in self.d.iteritems():
            ret.append('{state: >9} {bar} {count}'.format(
                state=state,
                bar='*' * (STARS * count / (self.total or 1)),
                count=count))
        return '\n'.join(ret)

    def __str__(self):
        """Distribute 100 stars over all the state, modulo rounding errors."""
        return self.histogram()

    def as_dict(self):
        """Return a dictionary with a key for total and for every bucket."""
        return dict(total=self.total, **self.d)

    def __nonzero__(self):
        return self.total != 0


def decode_es_datetime(es_datetime):
    """Turn an elasticsearch datetime into a datetime object."""
    try:
        return datetime.strptime(es_datetime, '%Y-%m-%dT%H:%M:%S')
    except ValueError:
        # For newer ES versions
        return datetime.strptime(es_datetime, '%Y-%m-%dT%H:%M:%S.%fZ')


def events_from_day(iso_day, es, size=1000000):
    """Return all Events from the given day, in order by room token and then
    timestamp."""
    # Optimize: Can I do this with aggregations and/or scripts or at least use
    # aggs to reduce it to rooms that ever have 2 people (whether
    # simultaneously or not) in them?
    hits = es.search({
        'query': {
            'filtered': {
                'query': {
                    'match_all': {}
                },
                'filter': {
                    'and': [
                        {'term': {'path': 'rooms'}},  # Assumption: "rooms" doesn't show up as a token (it's too short).
                        {'term': {'method': 'post'}},  # GETs are not usefully related to state flow.
                        {'range': {'code': {'gte': 200, 'lt': 300}}},  # no errors
                        {'exists': {'field': 'token'}},  # Assumption: token property is never present when unpopulated.
                        # Leave out unsupported iPad browsers, which
                        # nonetheless end up in our logs and show "join"
                        # actions. They have "iPad" as user_agent_os and a
                        # missing user_agent_browser field:
                        {'not': {'and': [{'term': {'user_agent_os.raw': 'iPad'}},
                                         {'not': {'exists': {'field': 'user_agent_browser.raw'}}}]}}
                    ]
                }
            }
        },
        'sort': ['token.raw', 'Timestamp'],
        'size': size,  # TODO: Slice nicely. As is, we get only 100K hits in a day, so YAGNI.
        '_source': {'include': ['action', 'token', 'userType', 'state',
                                'Timestamp', 'user_agent_browser',
                                'user_agent_version']}
    },
    index='loop-app-logs-%s' % iso_day,
    doc_type='request.summary')['hits']['hits']

    for hit in hits:
        source = hit['_source']
        action_and_state = source.get('action'), source.get('state')
        try:
            class_ = EVENT_CLASSES_BY_ACTION_AND_STATE[action_and_state]
        except KeyError:
            print "Unexpected action/state pair: %s" % (action_and_state,)
            continue
        yield class_(
            token=source['token'],
            is_clicker=source['userType'] == 'Link-clicker',  # TODO: Make sure there's nothing invalid in this field.
            timestamp=decode_es_datetime(source['Timestamp']),
            browser=source['user_agent_browser'],
            version=source['user_agent_version'])


class Event(object):
    # progression: An int whose greater magnitude represents more advanced
    # progression through the room states, culminating in sendrecv. Everything
    # is pretty arbitrary except that SendRecv has the max.

    def __init__(self, token, is_clicker, timestamp, browser=None, version=None):
        self.token = token
        self.is_clicker = is_clicker
        self.timestamp = timestamp
        self.browser = browser
        self.version = version  # int or None

    def __str__(self):
        return '<%s %s by %s at %s>' % (
            self.__class__.__name__,
            self.token, 'clicker' if self.is_clicker else 'built-in',
            self.timestamp)

    def __repr__(self):
        return self.__str__()

    def is_close_to_midnight(self):
        """Return whether I am so close to midnight that the segment
        containing me may span into the next day."""
        return self.timestamp + timedelta(seconds=TIMEOUT_SECS) >= self.next_midnight()

    def next_midnight(self):
        """Return the next midnight after me."""
        return datetime(self.timestamp.year,
                        self.timestamp.month,
                        self.timestamp.day) + timedelta(days=1)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    @classmethod
    def name(cls):
        return cls.__name__.lower()

    @property
    def firefox_version(self):
        """Return the integral Firefox major version that sent me.

        If it isn't Firefox at all, return 0.

        """
        return self.version if self.browser == 'Firefox' else 0


class Timeout(Event):
    """A virtual event we materialize to represent a user timing out"""
    progression = -100


class Refresh(Event):
    # This has a very low precedence because it happens every 5 minutes,
    # regardless of progression through other states. We wouldn't want it to
    # obscure more interesting states.
    progression = -3


class Leave(Event):
    progression = -2


class Join(Event):
    progression = -1


class Waiting(Event):
    progression = 0


class Starting(Event):
    progression = 1


class Receiving(Event):
    progression = 2


class Sending(Event):
    progression = 3


class SendRecv(Event):
    progression = 4  # must be the max


EVENT_CLASSES_BY_ACTION_AND_STATE = {
    ('join', None): Join,
    ('leave', None): Leave,
    ('refresh', None): Refresh,
    ('status', 'waiting'): Waiting,
    ('status', 'starting'): Starting,
    ('status', 'receiving'): Receiving,
    ('status', 'sending'): Sending,
    ('status', 'sendrecv'): SendRecv
}
EVENT_CLASSES_BEST_FIRST = sorted(
    EVENT_CLASSES_BY_ACTION_AND_STATE.itervalues(),
    key=lambda e: e.progression,
    reverse=True)


class Segment(object):
    """An iterable of events, plus some metadata about the collection as a
    whole"""

    def __init__(self, events=None):
        """
        :arg events: A list of events for me to encompass
        """
        self._events = events or []

    def append(self, event):
        self._events.append(event)

    def extend(self, events):
        self._events.extend(events)

    def __nonzero__(self):
        return bool(self._events)

    def __iter__(self):
        return iter(self._events)

    def furthest_state(self):
        """Return the closest state to sendrecv reached in me.

        If the built-in user is using FF 40 or later, take advantage of the
        state reporting it provides, and demand that both the link-clicker and
        the built-in user reach a state before it is deemed the "furthest".

        If not, report on just the link-clicker.

        """
        clicker_events, built_in_events = fence(lambda e: e.is_clicker, self)
        clicker_states = set(type(e) for e in clicker_events)
        if min(e.firefox_version for e in built_in_events) >= 40:
            # If a browser < 40 is used at any point, don't expect too much of the
            # data.
            built_in_states = set(type(e) for e in built_in_events)
            states = built_in_states & clicker_states
        else:
            states = clicker_states
        try:
            return PROGRESSION_TO_EVENT_CLASS[max(s.progression for s in states)]
        except ValueError:
            import pdb;pdb.set_trace()

    def is_failure(self):
        """Return whether I have failed."""
        return self.furthest_state() is not SendRecv


class Participant(object):
    def __init__(self):
        # Whether I am in the room:
        self.in_room = False
        # The last network activity I did. If I've done none since I last
        # exited the room, None:
        self.last_event = None
        self.states_reached = set()

    def do(self, event):
        """Update my state as if I'd just performed an Event."""
        self.states_reached.add(type(event))
        if isinstance(event, Leave):
            self.in_room = False
            self.last_event = None
        else:  # Any other kind of event means he's in the room.
            # Refreshing means you're saying "I'm in the room!" Adding Refresh
            # drops the number of leaves-before-joins from 44 to 35 on a
            # sampling of 10K log entries.
            self.in_room = True
            self.last_event = event

    def advance_to(self, timestamp):
        """Note that this participant didn't make any network noise until such
        and such a time.

        If this causes him to timeout, return a Timeout.

        """
        last = self.last_event
        if last and timestamp - last.timestamp >= TIMEOUT_DURATION:
            self.in_room = False  # timed out
            self.last_event = None
            return Timeout(last.token,
                           last.is_clicker,
                           last.timestamp + TIMEOUT_DURATION)


PROGRESSION_TO_EVENT_CLASS = {
    cls.progression: cls
    for cls in EVENT_CLASSES_BY_ACTION_AND_STATE.itervalues()}


def fence(predicate, iterable):
    """Split an iterable into 2 lists according to a predicate."""
    pred_true = []
    pred_false = []
    for item in iterable:
        (pred_true if predicate(item) else pred_false).append(item)
    return pred_true, pred_false


class Bucket(object):
    """An abstraction to reading and writing to a single key in an S3 bucket"""

    def __init__(self, bucket_name, key, access_key_id, secret_access_key):
        s3 = S3Connection(aws_access_key_id=access_key_id,
                          aws_secret_access_key=secret_access_key)
        bucket = s3.get_bucket(bucket_name, validate=False)  # We lack the privs to validate, so the bucket had better exist.
        self._key = Key(bucket=bucket, name=key)

    def read(self):
        return self._read(self._key.get_contents_as_string())

    def write(self, data):
        self._key.set_contents_from_string(self._write(data))


class VersionedJsonBucket(Bucket):
    """An abstraction for reading and writing a JSON-formatted data structure
    to a single *existing* S3 key

    If we start to care about more keys, revise this to at least share
    a connection.

    """
    def _read(self, contents):
        """Return JSON-decoded contents of the S3 key and the version of its
        format."""
        # UTF-8 comes out of get_contents_as_string().
        # We expect the key to have something (like {}) in it in the first
        # place. I don't trust boto yet to not spuriously return ''.
        contents = json.loads(contents)
        return contents.get('version', 0), contents.get('metrics', [])

    def _write(self, data):
        """Save a JSON-encoded data structure to the S3 key."""
        # UTF-8 encoded:
        contents = {'version': VERSION, 'metrics': data}
        return json.dumps(contents, ensure_ascii=True, separators=(',', ':'))


class PickleBucket(Bucket):
    def _read(self, contents):
        return pickle.loads(contents)

    def _write(self, data):
        return pickle.dumps(data)


def days_between(start, end):
    """Yield each datetime.date in the interval [start, end)."""
    while start < end:
        yield start
        start += timedelta(days=1)  # safe across DST because dates have no concept of hours


class Room(object):
    """State machine tracking the progress of conversations in a chat room

    Play back join/leave activity, assuming each event happens at a unique
    time (which should be close enough to true, since we log to the thousandth
    of a sec) and seeing if any built-in client (of which there should be only
    1) is in the room when any link-clicker (of which there might be multiple
    though theoretically no more than 1 at a time) is.

    """
    def __init__(self):
        self._clear()

    def _clear(self):
        self.clicker = Participant()
        self.built_in = Participant()
        self.segment = Segment()
        self.in_session = False  # whether there are 2 people in the room

    def do(self, event):
        """Update my state as if ``event`` has happened, returning a segment
        if one has just finished."""

        def ordered_timeouts(doer_timeout, other_timeout):
            """Return the timeouts that happened, in chronological order."""
            timeouts = []
            if doer_timeout:
                timeouts.append(doer_timeout)
            if other_timeout:
                timeouts.append(other_timeout)
            timeouts.sort(key=lambda e: e.timestamp)
            return timeouts
# When a Participant leaves or times out, is his states_reached reset? Very probably.
        # Assumption: There is <=1 link-clicker and <=1 built-in client in
        # each room. This is not actually strictly true: it is possible for 2
        # browsers to join if they're signed into the same FF Account.
        doer, other = ((self.clicker, self.built_in) if event.is_clicker
                       else (self.built_in, self.clicker))

        # Timeouts will intrinsically be timestamped before the event:
        other_timeout = other.advance_to(event.timestamp)
        doer_timeout = doer.advance_to(event.timestamp)  # Detect doer crashes.
        doer.do(event)

        if self.in_session:
            # Maybe a session ends:
            if not (self.clicker.in_room and self.built_in.in_room):
                self.in_session = False
                timeouts = ordered_timeouts(doer_timeout, other_timeout)
                if timeouts:
                    # The first of the one or more timeouts ended the segment.
                    # (Both timeouts will intrinsically have a timestamp
                    # before the doer's current event.)
                    self.segment.append(timeouts.pop(0))
                    # Start the next segment with the event after the ending
                    # timeout, whether it's another timeout or the event
                    # itself:
                    next_segment = timeouts + [event]
                else:
                    # It wasn't a timeout that ended the session; it must have
                    # been the event, so include it:
                    self.segment.append(event)
                    next_segment = Segment()

                ret, self.segment = self.segment, next_segment
                return ret
            else:  # still in session, so nobody timed out
                self.segment.append(event)
        else:
            # Add timeouts to segment. We aren't in a session, so we can add
            # timeouts without worrying about whether they should end the
            # segment.
            timeouts = ordered_timeouts(doer_timeout, other_timeout)
            self.segment.extend(timeouts)

            self.segment.append(event)

            # Maybe a session begins:
            if self.clicker.in_room and self.built_in.in_room:
                self.in_session = True

            # clicker.in_room + built_in.in_room can be unequal to the
            # "participants" field of the log entry because I'm applying the
            # rule "Only 1 built-in client at a time in a room", and the app
            # server isn't. For instance, in a series of logs wherein a
            # built-in joins, refreshes, and then presumably crashes and joins
            # again (or another copy of the browser joins using the same FF
            # account), the participant field goes up to 2.

            # TODO: Think about reporting bad data if a session gets to
            # sendrecv *without* 2 people being in the room. That would be
            # weird (and likely chalked up to timestamp slop).

    def final_segment(self):
        """Inform the Room that no more events are coming until far enough
        away from the last one to trigger a timeout. If this knowledge of an
        upcoming drop in the number of participants causes a segment to
        complete, return it. Otherwise, return None.

        """
        FAR_FUTURE = datetime(3000, 1, 1, 0, 0, 0)
        if self.in_session:
            # Get soonest Timeout event, and append it to the segment:
            longest_since_spoke = min(
                [self.clicker, self.built_in],
                key=lambda x: getattr(x.last_event, 'timestamp', FAR_FUTURE))
            timeout = longest_since_spoke.advance_to(FAR_FUTURE)
            assert timeout
            self.segment.append(timeout)
            ret = self.segment
        else:
            ret = None
        self._clear()
        return ret


class World(object):
    """Serializable state of all potentially in-progress sessions at a point
    in time

    We group events into "session segments", which are periods during which 2
    people attempt to join a room, though they may or may not succeed in
    negotiating bidirectional communication (a "session"). Session segments
    start with 0, 1, or 2 participants, always reach 2 at some point, and then
    descend to < 2. A session segment's last event is the one that causes its
    room to drop from 2 participants to < 2. If the "event" that causes this
    is a timeout, we materialize a virtual Timeout and stick it on the end of
    the segment so we can easily count which segments timed out. Thus, each
    segment ends with < 2 participants, and the first event of the next
    segment may raise it back to 2 again. If a room never has 2 people in it
    at once, it has no segments.

    We require network activity every 5 minutes, so any room without activity
    after 23:55 doesn't need to have anything carried over to the next day.
    The rest must have their state persisted into the next day. We stick the
    portion of the last session segment that falls within the previous day,
    along with Participant state, into a dict and pull it out on the next
    day's encounter of the same room to construct a complete session segment.

    Each day's bucket consists of session segments that end (so we
    don't have to go back and change any days we've already done) on that day.
    Instead of counting how many rooms ever had a sendrecv, we count how many
    sessions ever had one. This seems the most useful daily bucketing in the
    face of rooms and sessions that may span many days.

    In order to know the beginning state of the rooms that span midnights when
    computing just one day (as is typical for the nightly cron job), we could
    either lay down the data somewhere (private, since room tokens are all you
    need to join a room) or recompute the whole previous day as well (or some
    portion of it), hoping to hit a determinable number of participants at
    some point. In the latter case, sessions that span entire days may throw
    us off. We do the former, for greatest accuracy.

    """
    def __init__(self):
        self._rooms = {}  # {token: Room} for each room with a partial segment that ended suspiciously close to midnight at the end of the last day for which segments() was called

    def _non_spanning_segments(self, a_days_events, todays_rooms):
        """Yield segments that end on a given day and don't end close enough
        to a midnight to threaten to span into the next day.

        Squirrel any trailing partial segments away for later resumption.

        :arg todays_rooms: Set to which we add the room tokens we saw. This
            lets a caller pick out rooms whose last segment ended yesterday
            suspiciously close to midnight but then had no continuation today

        """
        for token, a_rooms_events in groupby(a_days_events, lambda e: e.token):  # for each room
            todays_rooms.add(token)
            if token == '-nGKotxv31M':
                a_rooms_events = list(a_rooms_events)
                import pdb;pdb.set_trace()

            room = self._rooms.pop(token, Room())  # grab saved state, if any
            for event in a_rooms_events:
                segment = room.do(event)  # update Participants, return a segment if it just finished (and clear internal segment buffer), None otherwise
                if segment:
                    yield segment  # Let the caller count the total segs and see how many made it to sendrecv.
            if not segment:  # We didn't happen to end on a segment boundary.
                if event.is_close_to_midnight():
                    self._rooms[token] = room  # save the partially done segment for tomorrow
                else:
                    final_segment = room.final_segment()
                    if final_segment:
                        yield final_segment  # return whatever's in there, and clear it

    def do(self, a_days_events):
        """Update the world's state to take into account a day's events, and
        yield the segments wholly contained by that day, as well as any
        spilling over midnight from the previous day.

        Yield segments. This is a segment::

            [event 1, event 2, event 3, ...]

        Segments are not necessarily returned in order.

        """
        yesterdays_rooms = set(self._rooms.iterkeys())
        todays_rooms = set()
        for segment in self._non_spanning_segments(a_days_events, todays_rooms):
            yield segment

        # Emit any last segment from each room whose final segment ran up
        # suspiciously close to midnight last night but then had no
        # continuation on the other side. (If it had a continuation, all
        # appropriate segments would have been already yielded by
        # _non_spanning_segments().)
        for token in yesterdays_rooms - todays_rooms:  # If a room had events again today and is spanning the next midnight, don't emit its leftovers.
            room = self._rooms.pop(token)
            final_segment = room.final_segment()
            if final_segment:
                yield final_segment  # return whatever's in there, and clear it


def counts_for_day(segments):
    """Return a StateCounter conveying a histogram of the segments' furthest
    states."""
    counter = StateCounter(['leave', 'refresh', 'join', 'waiting', 'starting',
                            'receiving', 'sending', 'sendrecv'])
    for segment in segments:
        furthest = segment.furthest_state()
        counter.incr(furthest.name)
    return counter


def success_duration_histogram(segments):
    """Return an iterable of lengths of time it takes to get from tryst to
    sendrecv."""
    # Answer: 90% in <7s, 99% in <23s, from running against most of 8/13/2015. I guess I want to see if the failures are more slanted toward short intervals than this histogram.
    for segment in segments:
        room = Room()
        start = None
        for event in segment:
            room.do(event)
            if start is None and room.in_session:
                start = event.timestamp
            if isinstance(event, SendRecv):
                if start is None:
                    yield 0  # Weirdness. Timestamp slop?
                else:
                    yield (event.timestamp - start).seconds
                break


def failure_duration_histogram(segments):
    """Return an iterable of at-least durations from when 2 people tryst to when
    there's only 1 person in the room.

    Actual duration is *at least* what's returned, almost certainly more, but
    we don't have easy access to the finishing event, which is in the next
    segment. But the point here is to see if there are an unusual number of 0s
    in the output, as in things didn't have enough time to negotiate a
    connection.

    """
    # A full 52% of these come out as 0. That suggests a lot failures could be due to having insufficient time for negotiation (though, of course, it really means "at least 0", not exactly 0, so take that into account. Next, it would be nice to get actual numbers for this, not just "at least" ones.
    for segment in segments:
        room = Room()
        start = None
        for event in segment:
            room.do(event)
            if room.in_session:
                start = event.timestamp
                break
        if start is not None:  # otherwise, 2 people never met. Impossible?
            yield (segment[-1].timestamp - start).seconds
        else:
            yield "Inconceivable!"


def update_metrics(es, version, metrics, world):
    """Update metrics with today's (and previous missed days') data.

    Also update the state of the ``world`` with sessions that may hang over
    into tomorrow.

    If VERSION has increased or ``metrics`` is empty, start over.

    """
    today = date.today()
    yesterday = today - timedelta(days=1)

    if not metrics or VERSION > version:  # need to start over
        start_at = BEGINNING_OF_TIME
        metrics = []
        world = World()
    else:
        # Figure out which days we missed, as of the end of the stored JSON.
        # (This tolerates unreliable cron jobs, which Heroku warns of, and
        # also guards against other transient failures.)
        start_at = datetime.strptime(metrics[-1]['date'], '%Y-%m-%d').date() + timedelta(days=1)

    # Add each of those to the bucket:
    for day in days_between(start_at, today):
        iso_day = day.isoformat()
        print "Computing furthest-state histogram for %s..." % iso_day

        segments = world.do(events_from_day(iso_day, es))
        counts = counts_for_day(segments)
        print counts
        print "%s sessions span midnight (%s%%)." % (len(world._rooms), len(world._rooms) / float(counts.total) * 100)
        a_days_metrics = counts.as_dict()
        a_days_metrics['date'] = iso_day
        metrics.append(a_days_metrics)

    return metrics, world


def main():
    """Pull the JSON of the historical metrics out of S3, compute the new ones
    up through yesterday, and write them back to S3.

    We can assume the JSON is small enough to handle because it's already
    being pulled into the browser in its entirety, along with a dozen other
    datasets, to display the dashboard.

    """
    es = ElasticSearch(environ['ES_URL'],
                       username=environ['ES_USERNAME'],
                       password=environ['ES_PASSWORD'],
                       ca_certs=join(dirname(__file__), 'mozilla-root.crt'),
                       timeout=600)

    # Get previous metrics and midnight-spanning room state from buckets:
    metrics_bucket = VersionedJsonBucket(
        bucket_name='net-mozaws-prod-metrics-data',
        key='loop-server-dashboard/loop_full_room_progress.json',
        access_key_id=environ['METRICS_ACCESS_KEY_ID'],
        secret_access_key=environ['METRICS_SECRET_ACCESS_KEY'])
    version, metrics = metrics_bucket.read()
    world_bucket = PickleBucket(
        bucket_name='mozilla-loop-metrics-state',
        key='session-progress.pickle',
        access_key_id=environ['STATE_ACCESS_KEY_ID'],
        secret_access_key=environ['STATE_SECRET_ACCESS_KEY'])
    world = world_bucket.read()

    metrics, world = update_metrics(es, version, metrics, world)

    # Write back to the buckets:
    world_bucket.write(world)
    metrics_bucket.write(metrics)


if __name__ == '__main__':
    main()


# Observations:
#
# * Most rooms never see 2 people meet: 20K lonely rooms vs. 1500 meeting ones.
# * There are some sessions in which leaves happen without symmetric joins.
#   See if these occur near the beginning of days. Otherwise, I would expect
#   at least Refreshes every 5 minutes.
# * These numbers may be a little high because we're assuming all
#   link-clickers are the same link-clicker. When we start logging sessionID,
#   we can start distinguishing them. (hostname is the IP of the server, not
#   of the client.)
# * We could be nice and not expect a sendrecv to happen if the co-presence of
#   2 people lasts only a few seconds. Maybe we could chart the length of failed
#   sessions and figure out where n sigmas is.
