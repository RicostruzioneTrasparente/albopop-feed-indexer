# -*- coding: utf-8 -*-

import sys, logging, requests, importlib, json, uuid
from datetime import datetime
from threading import Thread
from queue import Queue, Empty
from elasticsearch import Elasticsearch, helpers
ajc = importlib.import_module("albopop-json-converter.AlbopopJsonConverter")
converter = ajc.AlbopopJsonConverter()

sources_url = "https://raw.githubusercontent.com/RicostruzioneTrasparente/rt-scrapers/master/sources.json"
cache_file = "sources.json"
workers = 5
qsources = Queue()
qitems = Queue()
qenclosures = Queue()
download_dir = "./downloads"
chunk_size = 1024*1024 # 1MB
prefix = "albopop-v4-"

# Cache management
try:
    with open(cache_file) as f:
        sources = json.load(f)
except:
    logging.warning("Cache file not loaded!")
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

except Exception as e:
    logging.warning("Fetching of remote sources failed! %s" % e)

# Fetch feed from each source, target of a thread
def fetch(i,qs,qi,qe):

    logging.warning("Start fetcher %d" % i)

    # Stay alive until input queue is not empty
    while not qs.empty():

        try:
            source = qs.get()
        except Empty:
            continue

        r = requests.get(source['feed'], stream = True)
        r.raw.decode_content = True
        logging.warning("Fetch from %s: %d" % (r.url,r.status_code))

        # Get items from converted feed and put them in output queue
        jfeed = converter.xml2json(r.raw)
        for item in converter.get_items(jfeed):

            # Generate an universal unique id for item
            item_uuid = uuid.uuid4()
            item['uuid'] = str(item_uuid)

            # Loop on enclosures
            for index in range(len(item.get('enclosure',[]))):
                # Generate an universal unique id for enclosure
                enclosure_uuid = uuid.uuid4()
                item['enclosure'][index]['uuid'] = str(enclosure_uuid)
                # The file name of downloaded enclosure is the enclosure uuid in the namespace of the item uuid
                item['enclosure'][index]['filename'] = "%s.%s" % (
                    uuid.uuid5(item_uuid,str(enclosure_uuid)),
                    item['enclosure'][index]['type'].split('/')[-1]
                )
                # Put enclosure in download queue
                logging.warning("Put enclosure %s" % item['enclosure'][index]['uuid'])
                qe.put(item['enclosure'][index])

            # Put item in index queue
            logging.warning("Put item %s" % item['uuid'])
            qi.put(item)

        qs.task_done()

    logging.warning("Finish fetcher %d" % i)

# Download enclosures
def download(i,qe,qi,qs):

    logging.warning("Start downloader %d" % i)

    # Stay alive until input queue is not empty
    while not ( qe.empty() and qi.empty() and qs.empty() ):

        try:
            enclosure = qe.get(timeout = 1)
        except Empty:
            continue

        logging.warning("Download %s from %s" % (enclosure['filename'],enclosure['url']))
        r = requests.get(enclosure['url'], stream = True)

        try:
            with open(download_dir+'/'+enclosure['filename'],'wb') as f:
                for chunk in r.iter_content(chunk_size):
                    f.write(chunk)
        except Exception as e:
            logging.error("Enclosure download failed: %s" % e)

        qe.task_done()

    logging.warning("Finish downloader %d" % i)

# Generator to consume items queue
def items(qi,qs):

    while not ( qi.empty() and qs.empty() ):

        try:
            item = qi.get(timeout = 10)
        except Empty:
            continue

        enclosures = item.pop('enclosure',[])
        index = prefix + datetime.strptime(item['pubDate'],'%a, %d %b %Y %H:%M:%S %z').strftime('%Y.%m')

        logging.warning("Index item %s" % item['uuid'])
        yield {
            '_op_type': 'index',
            '_index': index,
            '_type': 'item',
            '_id': item['uuid'],
            '_source': item
        }

        for enclosure in enclosures:

            logging.warning("Index enclosure %s" % enclosure['uuid'])
            yield {
                '_op_type': 'index',
                '_index': index,
                '_type': 'enclosure',
                '_id': enclosure['uuid'],
                '_source': enclosure,
                '_parent': item['uuid']
            }

        qi.task_done()


# Populate sources queue
for source in sources.values():
    qsources.put(source)

# Run a pool of threads to consume sources queue and
# populate items one
fetchers = []
for i in range(workers):

    t = Thread(
        name = "fetcher_%d" % i,
        target = fetch,
        args = (i,qsources,qitems,qenclosures)
    )

    fetchers.append(t)
    t.start()

# Wait for fetchers
for t in fetchers:
    t.join()

# Run a pool of threads to consume enclosures queue and
# download all files
downloaders = []
for i in range(workers):

    t = Thread(
        name = "downloader_%d" % i,
        target = download,
        args = (i,qenclosures,qitems,qsources)
    )

    downloaders.append(t)
    t.start()

# Bulk index all items and enclosures
es = Elasticsearch(timeout = 60, retry_on_timeout = True)
helpers.bulk(
    es,
    items(qitems,qsources)
)

# Wait for downloaders
for t in downloaders:
    t.join()

