# -*- coding: utf-8 -*-

import sys, logging, requests, importlib, json
#from multiprocessing import Process, Pool, Queue
from queue import Queue, Empty
ajc = importlib.import_module("albopop-json-converter.AlbopopJsonConverter")
converter = ajc.AlbopopJsonConverter()

sources_url = "https://raw.githubusercontent.com/RicostruzioneTrasparente/rt-scrapers/master/sources.json"
cache_file = "sources.json"
#workers = 1
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

    for source_id in sources:
        if source_id not in new_ids:
            del sources[source_id]

    with open(cache_file,'w') as f:
        json.dump(sources, f)

except:
    logging.error("Fetching of remote sources failed!")

# Fetch feed from each source
def fetch(url):
    r = requests.get(url, stream = True)
    r.raw.decode_content = True
    logging.warning("Fetch from %s: %d" % (url,r.status_code))
    return converter.xml2json(r.raw)

def deliver(feed):
    for item in converter.get_items(feed):
        qitems.put(item)

#def mul(x):
#    logging.warning(x)
#    return x*x

#def cprint(x):
#    print(x)

#p = Pool(processes = workers)
#res = p.map_async(fetch, [ s['feed'] for s in sources.values() ], callback = deliver)
#res.wait()

for source in sources.values():
    deliver(fetch(source['feed']))

while not qitems.empty():
    print(qitems.get())

