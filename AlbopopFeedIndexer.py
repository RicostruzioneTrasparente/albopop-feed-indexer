# -*- coding: utf-8 -*-

import sys, logging, requests, importlib, json
from datetime import datetime
from threading import Thread
from queue import Queue, Empty
ajc = importlib.import_module("albopop-json-converter.AlbopopJsonConverter")
converter = ajc.AlbopopJsonConverter()

sources_url = "https://raw.githubusercontent.com/RicostruzioneTrasparente/rt-scrapers/master/sources.json"
cache_file = "sources.json"
workers = 2
qsources = Queue()
qitems = Queue()

# Cache management
try:
    with open(cache_file) as f:
        sources = json.load(f)
except:
    logging.error("Cache file not loaded!")
    sources = {}

# Remote sync
try:

    r = requests.get(sources_url)
    new_ids = []

    for new_source in r.json():
        source_id = new_source['id']
        new_ids.append(source_id)
        if new_source.get('feed'):
            if source_id in sources:
                sources[source_id].update(new_source)
            else:
                sources[source_id] = new_source

    for source_id in sources and source_id not in new_ids:
        del sources[source_id]

    with open(cache_file,'w') as f:
        json.dump(sources, f)

except:
    logging.error("Fetching of remote sources failed!")

# Fetch feed from each source, target of a thread
def fetch(i,iq,oq):

    # Stay alive until input queue is not empty
    while not iq.empty():

        try:
            source = iq.get()
        except Empty:
            continue

        r = requests.get(source['feed'], stream = True)
        r.raw.decode_content = True
        logging.warning("Fetch from %s: %d" % (source['feed'],r.status_code))

        # Get items from converted feed and put them in output queue
        jfeed = converter.xml2json(r.raw)
        for item in converter.get_items(jfeed):
            oq.put(item)

        iq.task_done()

# Populate sources queue
for source in sources.values():
    qsources.put(source)

# Run a pool of threads to consume sources queue and
# populate items one
threads = []
for i in range(workers):
    t = Thread(
        name = "fetcher_%d" % i,
        target = fetch,
        args = (i,qsources,qitems)
    )
    threads.append(t)
    t.start()

# Generator to consume items queue
def items(qi,qs):

    n1 = 0
    n2 = 0
    while not ( qi.empty() and qs.empty() ):

        try:
            item = qi.get(timeout = 10)
            qi.task_done()
            n1 += 1
            enclosures = item.pop('enclosure',[])
            item_id = n1
            yield {
                '_op_type': 'index',
                '_index': 'albopop-v4-'+datetime.strptime(item['pubDate'],'%a, %d %b %Y %H:%M:%S %z').strftime('%Y.%m'),
                '_type': 'item',
                '_id': item_id,
                'doc': item
            }
            for enclosure in enclosures:
                n2 += 1
                enclosure_id = n2
                yield {
                    '_op_type': 'index',
                    '_index': 'albopop-v4-'+datetime.strptime(item['channel']['pubDate'],'%a, %d %b %Y %H:%M:%S %z').strftime('%Y.%m'),
                    '_type': 'enclosure',
                    '_id': n2,
                    'doc': enclosure,
                    '_parent': n1
                }
        except Empty:
            continue

for item in items(qitems,qsources):
    print(item)

