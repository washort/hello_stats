from datetime import datetime

from nose.tools import ok_

from state_histogram import people_meet_in, Join, Leave, Refresh


def test_sessions():
    """We should recognize a 2-person simultaneous presence in all these
    scenarios."""
    ok_(people_meet_in([Join('a', False, datetime(2015, 1, 1, 0, 0, 0)),
                        Join('a', True, datetime(2015, 1, 1, 0, 4, 59))]))
    ok_(people_meet_in([Join('a', False, datetime(2015, 1, 1, 0, 0, 0)),
                        Leave('a', False, datetime(2015, 1, 1, 0, 0, 1)),
                        Join('a', True, datetime(2015, 1, 1, 0, 4, 59)),
                        Join('a', False, datetime(2015, 1, 1, 0, 5, 1))]))
