from datetime import datetime
from unittest import TestCase

from nose.tools import eq_, ok_

from state_histogram import Join, Leave, Refresh, Room, World


def test_is_close_to_midnight():
    # Not quite close to midnight yet:
    ok_(not Join('a', True, datetime(2015, 1, 1, 23, 54, 59)).is_close_to_midnight())
    # Exactly 5 minutes from midnight is close to midnight:
    ok_(Join('a', True, datetime(2015, 1, 1, 23, 55, 0)).is_close_to_midnight())
    # As is closer:
    ok_(Join('a', True, datetime(2015, 1, 1, 23, 59, 0)).is_close_to_midnight())
    # After midnight doesn't count as close:
    ok_(not Join('a', True, datetime(2015, 1, 1, 0, 0, 0)).is_close_to_midnight())


# These tests are kind of long and coupled; I didn't have time to write short
# ones. :-)
class RoomTests(TestCase):
    def test_2_segments(self):
        """Make sure Rooms recognize a a segment which ends with
        explicit Leaves and the 2nd of which times out."""
        r = Room()
        e1 = Join('a', True, datetime(2015, 1, 1, 1, 0, 0))
        e2 = Join('a', False, datetime(2015, 1, 1, 1, 0, 5))
        e3 = Leave('a', True, datetime(2015, 1, 1, 1, 0, 10))
        eq_(r.do(e1), None)
        eq_(r.do(e2), None)
        eq_(r.do(e3), [e1, e2])  # e3 will start next segment

        # Cause built-in participant to time out, just barely:
        e4 = Join('a', True, datetime(2015, 1, 1, 1, 5, 10))
        eq_(r.do(e4), None)
        eq_(r.built_in.in_room, False)

    def test_timeouts_with_final_segment(self):
        """All these cases should not result in 2-person sessions, since the first
        person times out before the second joins.

        Also test both final_segment() branches.

        """
        r = Room()
        e1 = Join('a', False, datetime(2015, 1, 1, 0, 0, 0))
        e2 = Join('a', True, datetime(2015, 1, 1, 0, 5, 1))
        e3 = Join('a', False, datetime(2015, 1, 1, 0, 5, 2))
        eq_(r.do(e1), None)
        eq_(r.do(e2), None)  # still not 2 participants, so no seg yet
        eq_(r.do(e3), None)  # nothing yet; at 2 and waiting to drop below
        eq_(r.final_segment(), [e1, e2, e3])

    def test_timeout_without_final_segment(self):
        """Make sure that, if the room never gets to 2 concurrent
        participants, we don't return a final segment."""
        r = Room()
        e1 = Join('a', False, datetime(2015, 1, 1, 0, 0, 0))
        e2 = Join('a', True, datetime(2015, 1, 1, 0, 5, 1))
        eq_(r.do(e1), None)
        eq_(r.do(e2), None)  # still not 2 participants, so no seg yet
        eq_(r.final_segment(), None)


class WorldTests(TestCase):
    def test_trifecta(self):
        """Make sure we properly handle a session entirely contained by Day A,
        a session spanning midnight, and one contained entirely by Day B."""
        w = World()

        # Segment contained by Day A:
        e1 = Join('a', False, datetime(2015, 1, 1, 0, 0, 0))  # times out
        e2 = Join('a', True, datetime(2015, 1, 1, 0, 5, 1))
        e3 = Join('a', False, datetime(2015, 1, 1, 0, 5, 2))  # times out

        # Segment spanning days:
        e4 = Join('a', True, datetime(2015, 1, 1, 23, 54, 0))
        e5 = Join('a', False, datetime(2015, 1, 1, 23, 58, 0))
        e6 = Refresh('a', True, datetime(2015, 1, 1, 23, 58, 30))

        # Segment contained by Day B:
        e7 = Leave('a', True, datetime(2015, 1, 2, 0, 1, 0))  # finish previous seg
        e8 = Join('a', True, datetime(2015, 1, 2, 9, 0, 0))
        e9 = Join('a', False, datetime(2015, 1, 2, 9, 1, 0))
        e10 = Leave('a', True, datetime(2015, 1, 2, 9, 2, 0))

        # Run Day A. The World should hang onto the spanning segment.
        eq_(list(w.do([e1, e2, e3, e4, e5, e6])), [[e1, e2, e3]])

        # Run Day B.
        eq_(list(w.do([e7, e8, e9, e10])), [[e4, e5, e6], [e7, e8, e9]])  # e10 is thrown away, as it's the one that causes participants to drop below 2, and then there are no more events to form a segment.
