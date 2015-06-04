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

Can I do it with aggregations and/or scripts?

"""
from collections import OrderedDict
import json

from blessings import Terminal
from pyelasticsearch import ElasticSearch

from hello_stats.settings import es_url, es_username, es_password


class StateCounter(object):
    def __init__(self):
        self.total = 0
        self.d = OrderedDict(waiting=0,
                             starting=0,
                             receiving=0,
                             sending=0,
                             sendrecv=0)

    def incr(self, state):
        self.d[state] += 1
        self.total += 1

    def __str__(self):
        """Distribute 100 stars over all the state, modulo rounding errors."""
        ret = []
        STARS = 100
        for state, count in self.d.iteritems():
            ret.append('{state: >9} {bar} {count}'.format(
                state=state,
                bar='*' * (STARS * count / (self.total or 1)),
                count=count))
        return '\n'.join(ret)


def main():
    term = Terminal()
    print term.clear
    counter = StateCounter()
    print counter, '\n'

    es = ElasticSearch(es_url, username=es_username, password=es_password, timeout=600)
    results = es.search({
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
        'size': 1000000,  # TODO: Slice nicely. As is, we get only 100K hits in a day, so YAGNI.
        '_source': {'include': ['Timestamp', 'token', 'action', 'userType', 'state']}
    },
    index='loop-app-logs-2015-06-01',
    doc_type='request.summary')
    
    # Get the day's records sorted by room token then Timestamp.
    # For one room token,
        # Do any link-clicker join/leave spans overlap with built-in ones?
        # If so, what's the latest state the link-clicker attained?
        # Add that to the counter.
    
    # TODO: What if there are multiple ppl trying to click the link? We should throw IP into the identifying tuple: (IP, clicker or FF, token).
    # TODO: Maybe pay attention to GET requests.

    print json.dumps(results, sort_keys=True, indent=4, separators=(',', ': '))


if __name__ == '__main__':
    main()
