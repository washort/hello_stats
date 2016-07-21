from datetime import date
import hashlib
import os
import urlparse

import psycopg2
from psycopg2.extras import Json
from pyelasticsearch import ElasticSearch

from hello_stats.events import hits_from_day

urlparse.uses_netloc.append("postgres")
url = urlparse.urlparse(os.environ["DATABASE_URL"])

conn = psycopg2.connect(
    database=url.path[1:],
    user=url.username,
    password=url.password,
    host=url.hostname,
    port=url.port
)


def row_data(Uuid, action, sessionId, roomConnectionId, hostname,
             userType, Timestamp, token, event=None, state=None,
             participants=None, user_agent_browser=None, user_agent_os=None,
             user_agent_version=None, **rest):
    if 'Hostname' in rest:
        del rest['Hostname']
    return Uuid, action, event, state, sessionId, roomConnectionId, hashlib.sha256(hostname + secret_salt).hexdigest(), participants, userType, Timestamp, token, user_agent_browser, user_agent_os, user_agent_version, Json(rest)

secret_salt = open("secret_salt.txt").read().strip()
url = open("es_url.txt").read().strip()
es = ElasticSearch(url, ca_certs="hello_stats/mozilla-root.crt")

out = hits_from_day(date(2016, 6, 28), es, 100000)
data = [x['_source'] for x in out]

cur = conn.cursor()
for i, row in enumerate(data):
    cur.execute("INSERT INTO loop_app_log (uuid, action, event, state, session_id, room_connection_id, hostname, participants, user_type, timestamp, token, user_agent_browser, user_agent_os, user_agent_version, extra) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row_data(**row))
    if (i % 100) == 0:
        print i
        conn.commit()
conn.commit()
