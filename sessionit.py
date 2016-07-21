import os
import urlparse
from itertools import groupby
import psycopg2
from hello_stats.events import EVENT_CLASSES_BY_ACTION_AND_STATE
from hello_stats.sessions import Room


def all_pg_events(cur):
    cur.execute("SELECT token, action, state, user_type = 'Link-clicker', timestamp, user_agent_browser, user_agent_version, session_id, event from loop_app_log order by token, timestamp")
    while True:
        morerows = cur.fetchmany()
        if not morerows:
            break
        for source in morerows:
            token, action, state, is_clicker, timestamp, browser, version, session_id, event = source
            try:
                class_ = EVENT_CLASSES_BY_ACTION_AND_STATE[action, state]
            except KeyError:
                print "Unexpected action/state pair: %s" % ((action, state),)
                continue
            yield class_(token, is_clicker, timestamp, session_id, browser, version, event or '')

def save(cur, sid, state):
    state_txt = state.__name__.lower()
    cur.execute("INSERT INTO loop_sessions (session_id, furthest_state) VALUES (%s, %s)", (sid, state_txt))


def build_sessions():
    urlparse.uses_netloc.append("postgres")
    url = urlparse.urlparse(os.environ["DATABASE_URL"])
    conn = psycopg2.connect(
        database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port
    )
    cur = conn.cursor()
    for token, a_rooms_events in groupby(all_pg_events(cur), lambda e: e.token):
        cur2 = conn.cursor()
        room = Room()
        seg = None
        for event in a_rooms_events:
            seg = room.do(event)
        if seg is not None:
            save(cur2, event.session_id, seg.furthest_state())
        else:
            seg = room.final_segment()
            if seg is not None:
                save(cur2, event.session_id, seg.furthest_state())
    conn.commit()
build_sessions()
