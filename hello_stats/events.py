from datetime import datetime, timedelta, date
import re


BEGINNING_OF_TIME = date(2015, 9, 5)

# Number of seconds without network activity to allow before assuming a room
# participant is timed out. When this many seconds goes by, they time out.
TIMEOUT_SECS = 60 * 5
TIMEOUT_DURATION = timedelta(0, TIMEOUT_SECS)


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
                                'user_agent_version', 'event']}
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
            browser=source.get('user_agent_browser', ''),
            version=source.get('user_agent_version', 0),
            event=source.get('event') or '')


class Event(object):
    # progression: An int whose greater magnitude represents more advanced
    # progression through the room states, culminating in sendrecv. Everything
    # is pretty arbitrary except that SendRecv has the max.

    def __init__(self, token, is_clicker, timestamp, browser=None, version=None, event=''):
        self.token = token
        self.is_clicker = is_clicker
        self.timestamp = timestamp
        self.browser = browser
        self.version = version if version else None  # int or None, never 0
        self.exception = self._exception_from_event(event)  # int or None

    def _exception_from_event(self, event):
        match = re.match(r'sdk\.exception\.(\d+)', event)
        if match:
            exception = int(match.group(1))
            if (not self.is_clicker
                    and self.firefox_version == 41
                    and exception == 1500):
                # FF 41 overreports exception 1500 on the browser side. Treat
                # it as if nothing went wrong.
                return None
            return exception

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

        If it isn't Firefox at all, return None.

        """
        return self.version if self.browser == 'Firefox' else None


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


class Success(object):
    """Fake state used to represent getting to SendRecv without an exception"""

    @classmethod
    def name(cls):
        return cls.__name__.lower()


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
EVENT_CLASSES_WORST_FIRST = sorted(
    EVENT_CLASSES_BY_ACTION_AND_STATE.itervalues(),
    key=lambda e: e.progression)
PROGRESSION_TO_EVENT_CLASS = {
    cls.progression: cls
    for cls in EVENT_CLASSES_BY_ACTION_AND_STATE.itervalues()}
