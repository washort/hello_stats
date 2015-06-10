"""Determine how often it occurs that 2 people are attempting to be in a room
together and actually succeed in communicating.

Usage::

    pip install blessings pyelasticsearch

    PYTHONPATH=.. python state_histogram.py 2015-05-26  # do a specific date

    PYTHONPATH=.. python state_histogram.py  # do today

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
from datetime import datetime
from itertools import groupby
import json
from textwrap import wrap
from sys import argv

from blessings import Terminal
from pyelasticsearch import ElasticSearch


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

    def __nonzero__(self):
        return self.total != 0


def logs_from_day(iso_date, es, size=1000000):
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
                        {'exists': {'field': 'token'}}  # Assumption: token property is never present when unpopulated.
                    ]
                }
            }
        },
        'sort': ['token', 'Timestamp'],
        'size': size,  # TODO: Slice nicely. As is, we get only 100K hits in a day, so YAGNI.
        '_source': {'include': ['action', 'token', 'userType', 'state', 'Timestamp']}
    },
    index='loop-app-logs-%s' % iso_date,
    doc_type='request.summary')['hits']['hits']

    for hit in hits:
        source = hit['_source']
        action_and_state = (source['action'], source.get('state'))
        try:
            class_ = EVENT_CLASSES[action_and_state]
        except KeyError:
            if action_and_state != ('status', None):  # We know about this one already.
                print "Unexpected action/state pair: %s" % (action_and_state,)
            continue
        yield class_(
            token=source['token'],
            is_clicker=source['userType'] == 'Link-clicker',  # TODO: Make sure there's nothing invalid in this field.
            timestamp=source['Timestamp'])


class Event(object):
    # state_num: An int whose greater magnitude represents more advanced
    # progression through the room states, culminating in sendrecv. Everything
    # is pretty arbitrary except that SendRecv has the max.

    def __init__(self, token, is_clicker, timestamp):
        self.token = token
        self.is_clicker = is_clicker
        self.timestamp = timestamp  # just for debugging

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

    built_in = False  # whether there's a built-in client in the room
    clicker = False  # whether there's a link-clicker in the room

    for event in events:
        # Assumption: There is <=1 link-clicker and <=1 built-in client in
        # each room. This is not actually strictly true: it is possible for 2
        # browsers to join if they're signed into the same FF Account.
        if isinstance(event, (Join, Refresh)):  # Assumption: Refreshing means you're saying "I'm in the room!" Adding Refresh drops the number of leaves-before-joins from 44 to 35 on a sampling of 10K log entries.
            if event.is_clicker:
                clicker = True
            else:
                built_in = True

            if clicker and built_in:
                return True
        elif isinstance(event, Leave):
            if event.is_clicker:
                clicker = False
            else:
                built_in = False

        # clicker + build_in can be unequal to the "participants" field of the
        # log entry because I'm applying the rule "Only 1 built-in client at a
        # time in a room", and the app server isn't. For instance, in a series
        # of logs wherein a built-in joins, refreshes, and then presumably
        # crashes and joins again (or another copy of the browser joins using
        # the same FF account), the participant field goes up to 2. Another
        # possibility happens shortly after midnight, when the index rolls
        # over but people are still in rooms.

            # TODO: Think about reporting bad data if a session gets to sendrecv *without* 2 people being in the room. That would be weird (and likely chalked up to timestamp slop).
    return False

    # TODO: timeouts
    # TODO: Maybe pay attention to GET requests.


NUM_TO_STATE = {cls.state_num: cls.__name__.lower()
                for cls in EVENT_CLASSES.values()}


def furthest_state(events):
    """Return the closest state to sendrecv reached in a series of events."""
    # TODO: In Firefox 40 and up, we start sending state events from the
    # built-in client, so demand a sendrecv from both users.
    return NUM_TO_STATE[max(e.state_num for e in events)]


def main(iso_date):
    """Compute a furthest-state histogram for the given date."""

    def print_wrapped(text):
        print '\n'.join(wrap(text, term.width))

    term = Terminal()
    counter = StateCounter(['leave', 'refresh', 'join', 'waiting', 'starting', 'receiving', 'sending', 'sendrecv'])
    weird_counter = StateCounter()
    es = ElasticSearch(es_url,
                       username=es_username,
                       password=es_password,
                       timeout=600)

    print "Computing furthest-state histogram for %s..." % iso_date

    # Get the day's records sorted by room token then Timestamp:
    events = logs_from_day(iso_date, es)  #, size=10000)
    for token, events in groupby(events, lambda l: l.token):
        # For one room token...
        events = list(events)  # prevent suffering
        try:
            if people_meet_in(events):
                counter.incr(furthest_state(events))
        except WeirdData as exc:
            counter.incr('weird')
            weird_counter.incr(exc.__class__)

    print_wrapped("Of rooms in which there were ever 2 people simultaneously, here are the furthest states reached by the link-clicker. (The link-clicker's state is easier to track. \"Further\" states are toward the bottom of the histogram.)")
    print counter, '\n'

    if weird_counter:
        print_wrapped("A breakdown of the weird data:")
        print weird_counter, '\n'


if __name__ == '__main__':
    try:
        from hello_stats.settings import es_url, es_username, es_password
    except ImportError:
        print 'Please edit settings.py before running me.'
    else:
        main(argv[1] if len(argv) >= 2 else datetime.now().date().isoformat())


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
# * There are about 60 action=status state=<empty> pairs in each day's logs.
#   What do those mean, if anything? They're all errors. Filter out bad HTTP status codes.
# * These numbers may be a little high because we're assuming all
#   link-clickers are the same link-clicker. When we start logging sessionID,
#   we can start distinguishing them. (hostname is the IP of the server, not
#   of the client.)
# * These numbers may be a little low because we don't yet notice timeouts
#   (client crashes, etc.), making the denominator falsely high.
