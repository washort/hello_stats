"""State machines that eat Events"""

from datetime import datetime
from itertools import groupby
from pprint import pformat

from hello_stats.events import EVENT_CLASSES_WORST_FIRST, PROGRESSION_TO_EVENT_CLASS, Leave, Timeout, TIMEOUT_DURATION


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

    def snapshotting(self, built_in, clicker):
        """Copy some state we want to preserve from the room participants.

        This accounts for situations in which, say, A joins, then B joins and
        leaves, then B rejoins. In that case, the 2nd segment would not contain
        any A events, but we still need to take A's reached states into
        account. A caller can use snapshotting() to pass them in. We copy so
        future mutations of the participants don't change our data.

        Return self.

        """
        self._built_in_states = built_in.states_reached.copy()
        self._min_firefox_version = built_in.min_firefox_version
        self._clicker_states = clicker.states_reached.copy()
        return self

    def __nonzero__(self):
        return bool(self._events)

    def __iter__(self):
        return iter(self._events)

    def __str__(self):
        return pformat(self._events)

    def furthest_state(self):
        """Return the closest state to sendrecv reached in me.

        If the built-in user is using FF 40 or later, take advantage of the
        state reporting it provides, and demand that both the link-clicker and
        the built-in user reach a state before it is deemed the "furthest".

        If not, report on just the link-clicker.

        """
        states = self._clicker_states.copy()
        if self._min_firefox_version and self._min_firefox_version >= 40:
            # If a browser < 40 is used at any point, don't expect too much of the
            # data.
            states &= self._built_in_states
            if not states:
                # This happens less than once in 1000 sessions.
                print 'No common states between built-in and clicker. This may be due to timing slop. Going with max state.'
                states = self._clicker_states | self._built_in_states
        return PROGRESSION_TO_EVENT_CLASS[max(s.progression for s in states)]

    def is_failure(self):
        """Return whether I have failed."""
        return self.furthest_state() is not SendRecv


class Participant(object):
    def __init__(self):
        self._clear_most()
        self._maybe_clear_rest()

    def _clear_most(self):
        """Clear the fields that should clear immediately upon leaving."""
        # Whether I am in the room:
        self.in_room = False
        # The last network activity I did. If I've done none since I last
        # exited the room, None:
        self.last_event = None
        self._just_cleared_most = True

    def _maybe_clear_rest(self):
        """Clear the fields that we let persist after a leave, just until the
        next join.

        We leave these fields alone when a participant leaves (or times out of)
        a room so we can still see what the furthest state reached (or the min
        FF version) was. They then get cleared automatically right before the
        next event.

        """
        if self._just_cleared_most:
            self._just_cleared_most = False
            self.states_reached = set()
            # Minimum FF version used by built-in client:
            self.min_firefox_version = None  # None or int > 0

    def do(self, event):
        """Update my state as if I'd just performed an Event."""
        self._maybe_clear_rest()
        self.states_reached.add(type(event))
        if not event.is_clicker and event.firefox_version:
            self.min_firefox_version = min(event.firefox_version,
                                           self.min_firefox_version or 10000)
        if isinstance(event, Leave):
            self._clear_most()
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
        self._maybe_clear_rest()
        last = self.last_event
        if last and timestamp - last.timestamp >= TIMEOUT_DURATION:  # timed out
            self._clear_most()
            return Timeout(last.token,
                           last.is_clicker,
                           last.timestamp + TIMEOUT_DURATION)


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

        # Assumption: There is <=1 link-clicker and <=1 built-in client in each
        # room. This is not actually strictly true: it is possible for 2
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
                    next_segment = Segment(timeouts + [event])
                else:
                    # It wasn't a timeout that ended the session; it must have
                    # been the event, so include it:
                    self.segment.append(event)
                    next_segment = Segment()

                ret, self.segment = self.segment, next_segment
                return ret.snapshotting(built_in=self.built_in,
                                        clicker=self.clicker)
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
            ret = self.segment.snapshotting(built_in=self.built_in,
                                            clicker=self.clicker)
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
