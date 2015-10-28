from datetime import datetime
from unittest import TestCase

from nose.tools import eq_, ok_

from state_histogram import Join, Leave, Refresh, SendRecv, Room, Segment, Sending, Starting, Timeout, Waiting, World


CLICKER = True
BUILT_IN = False


def test_is_close_to_midnight():
    # Not quite close to midnight yet:
    ok_(not Join('a', CLICKER, datetime(2015, 1, 1, 23, 54, 59)).is_close_to_midnight())
    # Exactly 5 minutes from midnight is close to midnight:
    ok_(Join('a', CLICKER, datetime(2015, 1, 1, 23, 55, 0)).is_close_to_midnight())
    # As is closer:
    ok_(Join('a', CLICKER, datetime(2015, 1, 1, 23, 59, 0)).is_close_to_midnight())
    # After midnight doesn't count as close:
    ok_(not Join('a', CLICKER, datetime(2015, 1, 1, 0, 0, 0)).is_close_to_midnight())


# These tests are kind of long and coupled; I didn't have time to write short
# ones. :-)
class RoomTests(TestCase):
    def test_2_segments(self):
        """Make sure Rooms recognize a segment which ends with explicit
        Leaves."""
        r = Room()
        e1 = Join('a', CLICKER, datetime(2015, 1, 1, 1, 0, 0))
        e2 = Join('a', BUILT_IN, datetime(2015, 1, 1, 1, 0, 5))
        e3 = Leave('a', CLICKER, datetime(2015, 1, 1, 1, 0, 10))
        eq_(r.do(e1), None)
        eq_(r.do(e2), None)
        eq_(r.do(e3)._events, [e1, e2, e3])

        # Cause built-in participant to time out, just barely:
        built_in_timeout1 = Timeout('a', BUILT_IN, datetime(2015, 1, 1, 1, 5, 5))
        e4 = Join('a', BUILT_IN, datetime(2015, 1, 1, 1, 6, 0))  # timed out and rejoined
        e5 = Join('a', CLICKER, datetime(2015, 1, 1, 1, 6, 5))
        eq_(r.do(e4), None)
        eq_(r.do(e5), None)
        # Time them both out:
        built_in_timeout2 = Timeout('a', BUILT_IN, datetime(2015, 1, 1, 1, 11, 0))
        eq_(r.final_segment()._events,
            [built_in_timeout1, e4, e5, built_in_timeout2])

    def test_timeouts_with_final_segment(self):
        """None of these cases should result in 2-person sessions, since the
        first person times out before the second joins.

        """
        r = Room()
        e1 = Join('a', BUILT_IN, datetime(2015, 1, 1, 0, 0, 0))
        timeout1 = Timeout('a', BUILT_IN, datetime(2015, 1, 1, 0, 5, 0))
        e2 = Join('a', CLICKER, datetime(2015, 1, 1, 0, 5, 1))
        e3 = Join('a', BUILT_IN, datetime(2015, 1, 1, 0, 5, 2))
        eq_(r.do(e1), None)
        eq_(r.do(e2), None)  # still not 2 participants, so no seg yet
        eq_(r.do(e3), None)  # nothing yet; at 2 and waiting to drop below
        timeout2 = Timeout('a', CLICKER, datetime(2015, 1, 1, 0, 10, 1))
        eq_(r.final_segment()._events, [e1, timeout1, e2, e3, timeout2])

    def test_timeout_without_final_segment(self):
        """Make sure that, if the room never gets to 2 concurrent
        participants, we don't return a final segment."""
        r = Room()
        e1 = Join('a', BUILT_IN, datetime(2015, 1, 1, 0, 0, 0))
        e2 = Join('a', CLICKER, datetime(2015, 1, 1, 0, 5, 1))
        eq_(r.do(e1), None)
        eq_(r.do(e2), None)  # still not 2 participants, so no seg yet
        eq_(r.final_segment(), None)


class WorldTests(TestCase):
    def test_trifecta(self):
        """Make sure we properly handle a session entirely contained by Day A,
        a session spanning midnight, and one contained entirely by Day B.

        Test a timeout spurred by an event that happens on the next day.

        """
        w = World()

        # Segment contained by Day A:
        e1 = Join('a', BUILT_IN, datetime(2015, 1, 1, 0, 0, 0))  # times out
        timeout1 = Timeout('a', BUILT_IN, datetime(2015, 1, 1, 0, 5, 0))
        e2 = Join('a', CLICKER, datetime(2015, 1, 1, 0, 5, 1))  # times out
        e3 = Join('a', BUILT_IN, datetime(2015, 1, 1, 0, 5, 2))  # times out
        timeout2 = Timeout('a', CLICKER, datetime(2015, 1, 1, 0, 10, 1))
        timeout3 = Timeout('a', BUILT_IN, datetime(2015, 1, 1, 0, 10, 2))

        # Segment spanning days:
        e4 = Join('a', CLICKER, datetime(2015, 1, 1, 23, 54, 0))
        e5 = Join('a', BUILT_IN, datetime(2015, 1, 1, 23, 58, 0))
        e6 = Refresh('a', CLICKER, datetime(2015, 1, 1, 23, 58, 30))
        e7 = Leave('a', CLICKER, datetime(2015, 1, 2, 0, 1, 0))  # on day B

        # Segment contained by Day B:
        timeout4 = Timeout('a', BUILT_IN, datetime(2015, 1, 2, 0, 3, 0))
        e8 = Join('a', CLICKER, datetime(2015, 1, 2, 9, 0, 0))
        e9 = Join('a', BUILT_IN, datetime(2015, 1, 2, 9, 1, 0))
        e10 = Leave('a', CLICKER, datetime(2015, 1, 2, 9, 2, 0))

        # Run Day A. The World should hang onto the spanning segment.
        eq_(list(segment._events for segment in w.do([e1, e2, e3, e4, e5, e6])),
           [[e1, timeout1, e2, e3, timeout2]]) # e1 and e2 are fine. e3 should go by quietly. e4 should cause a timeout of clicker at 10:1 and a timeout of built_in at 10:2.

        # Run Day B.
        eq_(list(segment._events for segment in w.do([e7, e8, e9, e10])),
            [[timeout3, e4, e5, e6, e7], [timeout4, e8, e9, e10]])


class FurthestStateTests(TestCase):
    """Tests for furthest_state()"""

    def test_ff_40(self):
        """Make sure we demand both the built-in client and the link-clicker
        reach a state before we consider it reached, as long as at least FF 40
        was used by the built-in participant."""
        events = [
            Join('a', BUILT_IN, datetime(2015, 1, 1, 0, 0, 0), 'Firefox', 40),
            Join('a', CLICKER, datetime(2015, 1, 1, 0, 0, 1), 'Firefox', 40),
            SendRecv('a', CLICKER, datetime(2015, 1, 1, 0, 0, 2), 'Firefox', 40)]
        world = World()
        segment = next(world.do(events))
        # They didn't both reach SendRecv:
        eq_(segment.furthest_state(), Join)

    def test_ff_39(self):
        """Go by only the linkclicker data if the built-in participant is using
        FF < 40."""
        events = [
            Join('a', BUILT_IN, datetime(2015, 1, 1, 0, 0, 0), 'Firefox', 39),
            Join('a', CLICKER, datetime(2015, 1, 1, 0, 0, 1), 'Firefox', 40),
            SendRecv('a', CLICKER, datetime(2015, 1, 1, 0, 0, 2), 'Firefox', 40)]
        world = World()
        segment = next(world.do(events))
        # They didn't both reach SendRecv:
        eq_(segment.furthest_state(), SendRecv)

    def test_solo_segment(self):
        """Take the other user's states into account even if there's only 1
        user in the segment.

        World.do() would sometimes return segments that contain only built-ins.
        This can happen if A joins, B joins, B leaves, then B rejoins. This
        test asserts that we can still get at the state info from the user A.

        """
        events = [
            Join('a', BUILT_IN, datetime(2015, 1, 1, 1, 0, 0), version=40, browser='Firefox'),
            Waiting('a', BUILT_IN, datetime(2015, 1, 1, 1, 0, 5), version=40, browser='Firefox'),

            Join('a', CLICKER, datetime(2015, 1, 1, 1, 0, 10)),
            Leave('a', CLICKER, datetime(2015, 1, 1, 1, 0, 20)),

            Join('a', CLICKER, datetime(2015, 1, 1, 1, 0, 30)),
            Waiting('a', CLICKER, datetime(2015, 1, 1, 1, 0, 35)),
            Leave('a', CLICKER, datetime(2015, 1, 1, 1, 0, 40))]
        world = World()
        last_segment = list(world.do(events))[-1]
        eq_(last_segment.furthest_state(), Waiting)

    def test_empty_intersection(self):
        """If the intersection betweeen built-in and clicker states is empty,
        perhaps due to time slop and consequent bad event ordering, be
        charitable and go with the most advanced state, as we would like to
        avoid false failures."""
        events = [
            Join('a', BUILT_IN, datetime(2015, 1, 1, 1, 0, 5), version=40, browser='Firefox'),
            Waiting('a', BUILT_IN, datetime(2015, 1, 1, 1, 0, 10), version=40, browser='Firefox'),
            Sending('a', BUILT_IN, datetime(2015, 1, 1, 1, 0, 15), version=40, browser='Firefox'),

            Join('a', CLICKER, datetime(2015, 1, 1, 1, 0, 20)),

            # Leave and Start are switched due to time slop:
            Leave('a', BUILT_IN, datetime(2015, 1, 1, 1, 0, 25), version=40, browser='Firefox'),
            Starting('a', BUILT_IN, datetime(2015, 1, 1, 1, 0, 30), version=40, browser='Firefox'),

            Leave('a', CLICKER, datetime(2015, 1, 1, 1, 0, 35))
            ]
        world = World()
        last_segment = list(world.do(events))[-1]
        eq_(last_segment.furthest_state(), Starting)
