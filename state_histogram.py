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

from boto import connect_s3
from boto.s3.key import Key
from pyelasticsearch import ElasticSearch


# Update this, and the next update will wipe out all saved data and start from
# scratch. A good reason to do this is if you improve the accuracy of the
# computations.
VERSION = 5

# The first date for which data for this metric was available in ES is 4/30.
# Before this, the userType field is missing. I suspect the data is wrong on
# 4/30, as the sendrecv rate is ridiculously low, so I'm starting at 5/1.
BEGINNING_OF_TIME = date(2015, 6, 12)

# Number of seconds without network activity to allow before assuming a room
# participant is timed out. When this many seconds goes by, they time out.
TIMEOUT_SECS = 60 * 5


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
        '_source': {'include': ['action', 'token', 'userType', 'state', 'Timestamp']}
    },
    index='loop-app-logs-%s' % iso_day,
    doc_type='request.summary')['hits']['hits']

    for hit in hits:
        source = hit['_source']
        action_and_state = source.get('action'), source.get('state')
        try:
            class_ = EVENT_CLASSES[action_and_state]
        except KeyError:
            print "Unexpected action/state pair: %s" % (action_and_state,)
            continue
        yield class_(
            token=source['token'],
            is_clicker=source['userType'] == 'Link-clicker',  # TODO: Make sure there's nothing invalid in this field.
            timestamp=decode_es_datetime(source['Timestamp']))


class Event(object):
    # state_num: An int whose greater magnitude represents more advanced
    # progression through the room states, culminating in sendrecv. Everything
    # is pretty arbitrary except that SendRecv has the max.

    def __init__(self, token, is_clicker, timestamp):
        self.token = token
        self.is_clicker = is_clicker
        self.timestamp = timestamp

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
        midnight = datetime(self.timestamp.year,
                            self.timestamp.month,
                            self.timestamp.day) + timedelta(days=1)
        return self.timestamp + timedelta(seconds=TIMEOUT_SECS) >= midnight


class Join(Event):
    state_num = -1


class Leave(Event):
    state_num = -2


class Waiting(Event):
    state_num = 0


class Starting(Event):
    state_num = 1


class Receiving(Event):
    state_num = 2


class Sending(Event):
    state_num = 3


class SendRecv(Event):
    state_num = 4  # must be the max


class Refresh(Event):
    state_num = -3


EVENT_CLASSES = {
    ('join', None): Join,
    ('leave', None): Leave,
    ('refresh', None): Refresh,
    ('status', 'waiting'): Waiting,
    ('status', 'starting'): Starting,
    ('status', 'receiving'): Receiving,
    ('status', 'sending'): Sending,
    ('status', 'sendrecv'): SendRecv
}


class Participant(object):
    def __init__(self):
        # Whether I am in the room:
        self.in_room = False
        # The last time I did any network activity:
        self.last_action = datetime(2000, 1, 1)

    def do(self, event):
        """Update my state as if I'd just performed an Event."""
        if isinstance(event, Leave):
            self.in_room = False
        else:  # Any other kind of event means he's in the room.
            # Refreshing means you're saying "I'm in the room!" Adding Refresh
            # drops the number of leaves-before-joins from 44 to 35 on a
            # sampling of 10K log entries.
            self.in_room = True
        self.last_action = event.timestamp

    def advance_to(self, timestamp):
        """Note that this participant didn't make any network noise until such
        and such a time."""
        if timestamp - self.last_action >= timedelta(0, TIMEOUT_SECS):
            self.in_room = False  # timed out


NUM_TO_STATE = {cls.state_num: cls.__name__.lower()
                for cls in EVENT_CLASSES.values()}


def furthest_state(events):
    """Return the closest state to sendrecv reached in a series of events."""
    # TODO: In Firefox 40 and up, we start sending state events from the
    # built-in client, so demand a sendrecv from both users.
    return NUM_TO_STATE[max(e.state_num for e in events)]


class VersionedJsonBucket(object):
    """An abstraction for reading and writing a JSON-formatted data structure
    to a single *existing* S3 key

    If we start to care about more keys, revise this to at least share
    a connection.

    """
    def __init__(self, bucket_name, key):
        """Auth credentials come from the env vars AWS_ACCESS_KEY_ID and
        AWS_SECRET_ACCESS_KEY."""
        s3 = connect_s3()
        bucket = s3.get_bucket(bucket_name, validate=False)  # We lack the privs to validate, so the bucket had better exist.
        self._key = Key(bucket=bucket, name=key)

    def read(self):
        """Return JSON-decoded contents of the S3 key and the version of its
        format."""
        # UTF-8 comes out of get_contents_as_string().
        # We expect the key to have something (like {}) in it in the first
        # place. I don't trust boto yet to not spuriously return ''.
        contents = json.loads(self._key.get_contents_as_string())
        return contents.get('version', 0), contents.get('metrics', [])

    def write(self, data):
        """Save a JSON-encoded data structure to the S3 key."""
        # UTF-8 encoded:
        contents = {'version': VERSION, 'metrics': data}
        self._key.set_contents_from_string(
            json.dumps(contents, ensure_ascii=True, separators=(',', ':')))


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
        self.segment = []
        self.in_session = False  # whether there are 2 people in the room

    def do(self, event):
        """Update my state as if ``event`` has happened, returning a segment
        if one has just finished."""
        self.segment.append(event)

        # Assumption: There is <=1 link-clicker and <=1 built-in client in
        # each room. This is not actually strictly true: it is possible for 2
        # browsers to join if they're signed into the same FF Account.
        doer, other = ((self.clicker, self.built_in) if event.is_clicker
                       else (self.built_in, self.clicker))
        doer.do(event)
        other.advance_to(event.timestamp)

        if not self.in_session:
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
        else:
            # Maybe a session ends:
            if not (self.clicker.in_room and self.built_in.in_room):
                self.in_session = False
                ret = self.segment
                self.segment = [ret.pop()]  # Don't return session-ending event; keep it to start the next segment.
                return ret

    def final_segment(self):
        """Inform the Room that no more events are coming, at least none
        before the room completely resets due to timeouts for all users. If
        this knowledge of an upcoming drop in the number of participants
        causes a segment to complete, return it. Otherwise, return None.

        """
        ret = self.segment if self.in_session else None
        self._clear()
        return ret


class World(object):
    """Serializable state of all potentially in-progress sessions at a point
    in time

    We divide events into "session segments", whose endpoint is the event
    before the one that causes a room to drop from 2 participants to <2. (This
    lets us treat timeout-triggered ends the same as explicit Leaves. This
    causes each segment to begin with <2 participants and end with 2.) The
    next event begins the next session segment. If a room never has 2 people
    in it, it has no segments.

    We require network activity every 5 minutes, so any room without activity
    after 23:55 doesn't need to have anything carried over to the next day.
    The rest must have their state persisted into the next day. We stick the
    portion of the last session segment that falls within the previous day,
    along with Participant state, into a dict and pull it out on the next
    day's encounter of the same room to construct a complete session segment.

    Each day's bucket consists of session segments that end (since then we
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
        # In each segment, here are the furthest states reached by the
        # link-clicker. (The link-clicker's state is easier to track.)
        furthest = furthest_state(segment)
        counter.incr(furthest)
    # Of rooms in which there were ever 2 people simultaneously, here are the
    # furthest states reached by the link-clicker. (The link-clicker's state
    # is easier to track. "Further" states are toward the bottom of the
    # histogram.)"
    return counter


def update_metrics(es, version, metrics, world):
    """Update metrics with today's (and previous missed days') data.

    Also update the state of the ``world`` with sessions that may hang over
    into tomorrow.

    If VERSION has increased, start over.

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
        a_days_metrics = counts_for_day(segments).as_dict()
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
    bucket = VersionedJsonBucket(
        bucket_name='net-mozaws-prod-metrics-data',
        key='loop-server-dashboard/loop_full_room_progress.json')
    version, metrics = bucket.read()
    world = unpickle_world()

    metrics, world = update_metrics(es, version, metrics, world)

    # Write back to the buckets:
    pickle_world(world)
    bucket.write(metrics)


if __name__ == '__main__':
    main()


# Observations:
#
# * Most rooms never see 2 people meet: 20K lonely rooms vs. 1500 meeting ones.
# * There are many sessions consisting of nothing by Refresh events,
#   generally a mix of clickers and built-ins. Where are the joins? On a
#   previous day? It would be interesting to see if these happen near the
#   beginnings of days.
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
