from datetime import date
import hashlib
import os
import psycopg2
import urlparse

from hello_stats.events import hits_from_day
from pyelasticsearch import ElasticSearch

urlparse.uses_netloc.append("postgres")
url = urlparse.urlparse(os.environ["DATABASE_URL"])
conn = psycopg2.connect(
        database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port
    )


url = open("es_url.txt").read().strip()
es = ElasticSearch(url, ca_certs="hello_stats/mozilla-root.crt")

out = hits_from_day(date(2016, 6, 28), es, 100000)
data = [x['_source'] for x in out]

cur = conn.cursor()
for i, row in enumerate(data):
    cur.execute("INSERT INTO loop_app_log (uuid, action, event, session_id, room_connection_id, hostname, participants, user_type, timestamp, token, user_agent_browser, user_agent_os, user_agent_version) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", [row['Uuid'], row['action'], row.get('event'), row['sessionId'], row['roomConnectionId'], hashlib.sha256(row['hostname']).hexdigest(), row.get('participants'), row['userType'], row['Timestamp'], row['token'], row['user_agent_browser'], row['user_agent_os'], row['user_agent_version']])
    if (i % 10000) == 0:
        conn.commit()
conn.commit()
