"""Proposed questions to answer: (Bounce these off Adam.)

* What portion of users ever reach the send or recv states? This should indicate whether Moz's part of the setup succeeded. Do we already have this?
* How often does it occur that 2 people are attempting to be in a room (init->waiting->starting->sending/receiving and not timed out or otherwise disconnected (if we can tell)) but don't reach sendrecv so they can talk to each other?

Can I do it with aggregations?

"""
import json

from pyelasticsearch import ElasticSearch

from hello_stats.settings import es_url, es_username, es_password

es = ElasticSearch(es_url, username=es_username, password=es_password)
#print es.cluster_state(index='loop-simplepush-app-2015-05-10')
mapping = es.get_mapping(index='loop-app-logs-2015-05-29')
print json.dumps(mapping, sort_keys=True, indent=4, separators=(',', ': '))
