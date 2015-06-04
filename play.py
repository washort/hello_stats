"""Proposed questions to answer: (Bounce these off Adam.)

* What portion of users ever reach the send or recv states? This should indicate whether Moz's part of the setup succeeded. Do we already have this?
* How often does it occur that 2 people are attempting to be in a room (join----leave spans overlap)
(init->waiting->starting->sending/receiving and not timed out or otherwise disconnected (if we can tell)) but don't reach sendrecv so they can talk to each other?
* When join---leave spans overlap in a room, what is the furthest state the link-clicker reaches (since we can't tell how far the built-in client gets, and sendrecv implies at least one party has bidirectional flow)? count(*) those. Later: for *each* overlapping join---leave pair in a room, answer the same question.

Unregistered/Registered:
    action = join refresh* leave
    state = <none>
Link-clicker:
    action = join (status / refresh)+ leave
    state = waiting starting receiving sending? sendrecv  # These can come out of order due to varying latencies in transports.

Iff action=status, there's a state.

"""
from collections import OrderedDict
from itertools import groupby
import json
from sys import stdout

from blessings import Terminal
from pyelasticsearch import ElasticSearch

from hello_stats.settings import es_url, es_username, es_password


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
                        {'term': {'method': 'post'}},  # TODO: Would it help us calculate better timeouts to pay attention to GETs, or do they happen only early in the session?
                        {'exists': {'field': 'token'}}  # Assumption: token property is never present when unpopulated.
                    ]
                }
            }
        },
        'sort': ['token', 'Timestamp'],
        'size': size,  # TODO: Slice nicely. As is, we get only 100K hits in a day, so YAGNI.
        '_source': {'include': ['action', 'token', 'userType', 'state', 'hostname', 'Timestamp']}
    },
    index='loop-app-logs-%s' % iso_date,
    doc_type='request.summary')['hits']['hits']

    for hit in hits:
        source = hit['_source']
        action_and_state = (source['action'], source.get('state'))
        try:
            class_ = EVENT_CLASSES[action_and_state]
        except KeyError:
            print "Unexpected action/state pair: %s" % (action_and_state,)
            continue
        yield class_(
            token=source['token'],
            is_clicker=source['userType'] == 'Link-clicker',  # TODO: Make sure there's nothing invalid in this field.
            hostname=source['hostname'].lstrip('ip-'),
            timestamp=source['Timestamp'])


class Event(object):
    # state_num: An int whose greater magnitude represents more advanced
    # progression through the room states, culminating in sendrecv. Everything
    # is pretty arbitrary except that SendRecv has the max.

    def __init__(self, token, is_clicker, hostname, timestamp):
        self.token = token
        self.is_clicker = is_clicker
        self.hostname = hostname
        self.timestamp = timestamp  # just for debugging

    def __str__(self):
        return '<%s %s by %s from %s at %s>' % (
            self.__class__.__name__,
            self.token, 'clicker ' if self.is_clicker else 'built-in',
            self.hostname,
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
    pass


class LeaveBeforeJoin(WeirdData):
    pass


def people_meet_in(events):
    """Return whether there were ever 2 people in a room simultaneously.

    :arg events: All the logged Events from the room, in time order

    """
    # Do any link-clicker join/leave spans overlap with built-in ones?
    # Play back join/leave activity, assuming each event happens at a unique time (which should be close enough to true) and seeing if any build-in client (of which there should be only 1) is in the room when any link-clicker (of which there might be multiple though theoretically no more than 1 at once) is.
    built_ins = set()  # {ip, ...}
    clickers = set()

    for event in events:
        whichever_set = clickers if event.is_clicker else built_ins
        if isinstance(event, Join):
            whichever_set.add(event.hostname)
            if clickers and built_ins:
                return True
        elif isinstance(event, Leave):
            try:
                whichever_set.remove(event.hostname)
            except KeyError as exc:
                raise LeaveBeforeJoin  # TODO: Maybe tolerate this.

            # TODO: Think about reporting bad data if a session gets to sendrecv *without* 2 people being in the room. That would be weird (and likely chalked up to timestamp slop).
    return False

    # TODO: timeouts
    # TODO: Maybe pay attention to GET requests.


NUM_TO_STATE = [cls.__name__.lower() for cls in
                sorted(EVENT_CLASSES.values(), key=lambda e: e.state_num)]


def furthest_state(events):
    """Return the closest state to sendrecv reached in a series of events."""
    return NUM_TO_STATE[max(e.state_num for e in events)]


def main():
    term = Terminal()
    counter = StateCounter(['waiting', 'starting', 'receiving', 'sending', 'sendrecv', 'weird'])
    weird_counter = StateCounter()
    es = ElasticSearch(es_url,
                       username=es_username,
                       password=es_password,
                       timeout=600)

    # Get the day's records sorted by room token then Timestamp:
    events = logs_from_day('2015-06-01', es, size=10000)
    for token, events in groupby(events, lambda l: l.token):
        # For one room token...
        events = list(events)  # prevent suffering
        try:
            if people_meet_in(events):
                counter.incr(furthest_state(events))
        except WeirdData as exc:
            counter.incr('weird')
            weird_counter.incr(exc.__class__)
    #print term.clear

    print "Of rooms in which there were ever 2 people simultaneously, here is the furthest state reached by the link-clicker. (The link-clicker's states are easier to track. \"Further\" states are toward the bottom of the histogram.)"
    print counter, '\n'

    print "A breakdown of the weird data:"
    print weird_counter, '\n'

    #stdout.flush()


if __name__ == '__main__':
    main()


# Observations:
# * Most rooms never see 2 people meet.
# * There are several sessions consisting of nothing by Refresh events,
#   generally a mix of clickers and built-ins. Where are the joins? On a
#   previous day?
# * Where are all the sendrecvs? Does furthest_state work right?
# * What's with all these LeaveBeforeJoins?