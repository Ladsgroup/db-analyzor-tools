import base64
import random
from functools import lru_cache

import requests

# Wikimedia servers reject requests without a user agent
session = requests.Session()
session.headers['User-Agent'] = 'db-analyzor-tools (https://github.com/Ladsgroup/db-analyzor-tools)'


class Gerrit(object):
    def __init__(self):
        self.url = 'https://gerrit.wikimedia.org/g/'

    def get_file(self, path):
        return self.get_url(self.url + path)

    def get_url(self, url):
        """Fetch a file from a gitiles URL."""
        res = session.get(url + '?format=TEXT')
        res.raise_for_status()
        return base64.b64decode(res.text).decode('utf-8')


@lru_cache(maxsize=None)
def _get_dblist(dblist):
    return Gerrit().get_file(
        'operations/mediawiki-config/+/master/dblists/{dblist}.dblist'.format(
            dblist=dblist))


def get_wikis_from_dblist(dblist, all_=False):
    dbs = _get_dblist(dblist).split('\n')
    random.shuffle(dbs)
    dbs_returning = []
    for line in dbs:
        if not line or line.startswith('#'):
            continue
        dbs_returning.append(line.strip())
        if not all_ and dblist == 's3':
            return [line.strip()]
    return sorted(dbs_returning)


def get_shard_mapping(dc):
    shard_mapping = {'hosts': {}, 'wikis': {}}
    db_data = session.get(
        'https://noc.wikimedia.org/dbconfig/{}.json'.format(dc)).json()
    for shard in db_data['sectionLoads']:
        cases = []
        if shard == 'DEFAULT':
            name = 's3'
        else:
            name = shard
        for type_ in db_data['sectionLoads'][shard]:
            cases += list(type_.keys())
        shard_mapping['hosts'][name] = cases
        wikis = get_wikis_from_dblist(name, True)
        for wiki in wikis:
            shard_mapping['wikis'][wiki] = name
    # External clusters (e.g. x1) only hold shared or extension tables
    for cluster, loads in db_data['externalLoads'].items():
        shard_mapping['hosts'][cluster] = [h for type_ in loads for h in type_]
    return shard_mapping


def get_table_catalog():
    """Catalog of production tables: their schema sources and placement."""
    return session.get(
        'https://config-master.wikimedia.org/mediawiki-tables.json').json()
