from datetime import date, timedelta

import json

import requests
from hello_stats.session_progress import StateCounter, World, days_between, events_from_day
from pyelasticsearch import ElasticSearch, ElasticHttpNotFoundError
url = open("es_url.txt").read().strip()
tokbox_cookie = json.load(open("tokbox_cookie.json"))

es = ElasticSearch(url, ca_certs="hello_stats/mozilla-root.crt")


def qualities_for_day(segments):
    """Return a StateCounter conveying a histogram of the segments' furthest
    states."""
    counter = StateCounter(['average+', 'good+', 'no data+',
                            'average-', 'good-', 'no data-'])
    for segment in segments:
        fs = segment.furthest_state()
        if fs.name() == "success":
            continue
        elif fs.name() == "sendrecv":
            tag = "-"
        else:
            continue
        try:
            qual = requests.get('https://tokbox.com/developer/tools/inspector/account/1589031/project/45552152/session/%s/getQuality' % segment._events[0].session_id, cookies=tokbox_cookie).json()['quality']
        except Exception, e:
            print "*SKIP*", e
        else:
            msg = qual + tag
            if msg == 'good-':
                print "**", segment._events[0].session_id
            counter.incr(msg)
    return counter


def update_metrics(es, version, metrics, world, beginning_of_time):
    """Update metrics with today's (and previous missed days') data.

    Also update the state of the ``world`` with sessions that may hang over
    into tomorrow.

    If VERSION has increased or ``metrics`` is empty, start over.

    """
    today = date.today()
    yesterday = today - timedelta(days=1)

    start_at = beginning_of_time
    metrics = []
    world = World()
    # Add each of those to the bucket:
    for day in days_between(start_at, today):
        iso_day = day.isoformat()
        print "Computing furthest-state histogram for %s..." % iso_day

        try:
            segments = world.do(events_from_day(iso_day, es))
            counts = qualities_for_day(segments)
        except ElasticHttpNotFoundError:
            print 'Index not found. Proceeding to next day.'
            continue
        print counts
        print "%s sessions span midnight (%s%%)." % (len(world._rooms), len(world._rooms) / float(counts.total) * 100)
        a_days_metrics = counts.as_dict()
        a_days_metrics['date'] = iso_day
        metrics.append(a_days_metrics)

    return metrics, world

metrics = update_metrics(es, 8, [], None, date(2016, 6, 27))
