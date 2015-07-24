#!/usr/bin/env python
"""Determine how often it occurs that 2 people are attempting to be in a room
together and actually succeed in communicating. Write that data to an S3
bucket so a dashboard can pull it out.

Usage::

    ES_URL=... ES_USERNAME=... ES_PASSWORD=... python state_histogram.py

Specifically, when join-leave spans overlap in a room, what is the furthest
state the link-clicker reaches (since we can't tell how far the built-in
client gets, and sendrecv implies at least one party has bidirectional flow)?
We draw a histogram of the answer to that question.

Possible later enhancement: for *each* overlapping join-leave pair in a room,
answer the same question.

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
VERSION = 4

# The first date for which data for this metric was available in ES is 4/30.
# Before this, the userType field is missing. I suspect the data is wrong on
# 4/30, as the sendrecv rate is ridiculously low, so I'm starting at 5/1.
BEGINNING_OF_TIME = date(2015, 5, 1)


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


def logs_from_day(iso_day, es, size=1000000):
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


class WeirdData(Exception):
    """There was something unexpected about the data that we want to count in
    a bucket."""


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
        if timestamp - self.last_action >= timedelta(0, 60 * 5):
            self.in_room = False  # timed out


def people_meet_in(events):
    """Return whether there were ever 2 people in a room simultaneously.

    In other words, do any link-clicker join/leave spans overlap with built-in
    ones?

    :arg events: All the logged Events from the room, in time order

    """
    # Play back join/leave activity, assuming each event happens at a unique
    # time (which should be close enough to true, since we log to the
    # thousandth of a sec) and seeing if any built-in client (of which there
    # should be only 1) is in the room when any link-clicker (of which there
    # might be multiple though theoretically no more than 1 at a time) is.

    built_in = Participant()  # built-in client
    clicker = Participant()  # link-clicker

    for event in events:
        # Assumption: There is <=1 link-clicker and <=1 built-in client in
        # each room. This is not actually strictly true: it is possible for 2
        # browsers to join if they're signed into the same FF Account.
        participant, other = ((clicker, built_in) if event.is_clicker
                              else (built_in, clicker))
        participant.do(event)
        other.advance_to(event.timestamp)
        if built_in.in_room and clicker.in_room:
            return True

        # clicker.in_room + built_in.in_room can be unequal to the
        # "participants" field of the log entry because I'm applying the rule
        # "Only 1 built-in client at a time in a room", and the app server
        # isn't. For instance, in a series of logs wherein a built-in joins,
        # refreshes, and then presumably crashes and joins again (or another
        # copy of the browser joins using the same FF account), the
        # participant field goes up to 2. Another possibility happens shortly
        # after midnight, when the index rolls over but people are still in
        # rooms.

        # TODO: Think about reporting bad data if a session gets to sendrecv
        # *without* 2 people being in the room. That would be weird (and
        # likely chalked up to timestamp slop).
    return False


NUM_TO_STATE = {cls.state_num: cls.__name__.lower()
                for cls in EVENT_CLASSES.values()}


def furthest_state(events):
    """Return the closest state to sendrecv reached in a series of events."""
    # TODO: In Firefox 40 and up, we start sending state events from the
    # built-in client, so demand a sendrecv from both users.
    return NUM_TO_STATE[max(e.state_num for e in events)]


class JsonBucket(object):
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


def metrics_for_day(day, es):
    """Compute and return a furthest-state histogram for the given date.

    :arg day: A datetime.date
    :arg es: An ElasticSearch connection

    """
    iso_day = day.isoformat()
    counter = StateCounter(['leave', 'refresh', 'join', 'waiting', 'starting',
                            'receiving', 'sending', 'sendrecv'])
    weird_counter = StateCounter()

    print "Computing furthest-state histogram for %s..." % iso_day

    # Get the day's records sorted by room token then Timestamp:
    events = logs_from_day(iso_day, es)  #, size=10000)
    for token, events in groupby(events, lambda l: l.token):
        # For one room token...
        events = list(events)  # prevent suffering
        try:
            if people_meet_in(events):
                counter.incr(furthest_state(events))
        except WeirdData as exc:  # for exploration
            counter.incr('weird')
            weird_counter.incr(exc.__class__)

    # Of rooms in which there were ever 2 people simultaneously, here are the
    # furthest states reached by the link-clicker. (The link-clicker's state
    # is easier to track. "Further" states are toward the bottom of the
    # histogram.)"
    totals = counter.as_dict()
    totals['date'] = iso_day

    if weird_counter:
        print "A breakdown of the weird data:"
        print weird_counter, '\n'

    return totals


def days_between(start, end):
    """Yield each datetime.date in the interval [start, end)."""
    while start < end:
        yield start
        start += timedelta(days=1)  # safe across DST because dates have no concept of hours


def update_s3(es):
    """Pull the JSON of the historical metrics out of S3, compute the new ones
    up through yesterday, and write them back to S3.

    We can assume the JSON is small enough to handle because it's already
    being pulled into the browser in its entirety, along with a dozen other
    datasets, to display the dashboard.

    """
    today = date.today()
    yesterday = today - timedelta(days=1)

    # Get stuff from bucket:
    bucket = JsonBucket(
        bucket_name='net-mozaws-prod-metrics-data',
        key='loop-server-dashboard/loop_full_room_progress.json')
    version, metrics = bucket.read()

    if not metrics or VERSION > version:  # need to start over
        start_at = BEGINNING_OF_TIME
        metrics = []
    else:
        # Figure out which days we missed, as of the end of the stored JSON. (This
        # tolerates unreliable cron jobs, which Heroku warns of, and also guards
        # against other transient failures.)
        start_at = datetime.strptime(metrics[-1]['date'], '%Y-%m-%d').date() + timedelta(days=1)

    # Add each of those to the bucket:
    for day in days_between(start_at, today):
        metrics.append(metrics_for_day(day, es))

    # Write back to the bucket:
    bucket.write(metrics)


def main():
    es = ElasticSearch(environ['ES_URL'],
                       username=environ['ES_USERNAME'],
                       password=environ['ES_PASSWORD'],
                       ca_certs=join(dirname(__file__), 'mozilla-root.crt'),
                       timeout=600)
    if environ.get('DEV'):
        # Skip the whole S3 dance, and just print the stats for a recent day:
        print metrics_for_day(date.today() - timedelta(days=2),
                              es)
    else:
        update_s3(es)


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
# We divide events into "session segments", whose endpoint is the event before the one that causes a room to drop from 2 participants to <2. (This lets us treat timeout-triggered ends the same as explicit Leaves. This causes each segment to begin with <2 participants and end with 2.) The next event begins the next session segment. If a room never has 2 people in it, it has no segments.
# We require network activity every 5 minutes, so any room without activity after 23:55 doesn't need to have anything carried over to the next day. The rest must have their state persisted into the next day. We should stick the portion of the last session segment that falls within the previous day, along with Participant state, into a dict and pull it out on the next day's encounter of the same room to construct a complete session segment. Hide all that behind an iterator (segments_from_days()).
# Each day's bucket consists of session segments that end (since then we don't have to go back and change any days we've already done) on that day. Instead of counting how many rooms ever had a sendrecv, we count how many sessions ever had one. This seems the most useful daily bucketing in the face of rooms and sessions that may span many days.
# In order to know the beginning state of the rooms that span midnights when computing just one day (as is typical for the nightly cron job), we can either lay down the data somewhere (private, since room tokens are all you need to join a room) or recompute the whole previous day as well (or some portion of it), hoping to hit a determinable number of participants at some point. In the latter case, sessions that span entire days may throw us off.
