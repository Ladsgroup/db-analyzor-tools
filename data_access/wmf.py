import base64
import random
import sys

import requests


class Gerrit(object):
    def __init__(self):
        self.url = 'https://gerrit.wikimedia.org/g/'

    def get_file(self, path):
        url = '{0}{1}?format=TEXT'.format(self.url, path)
        return base64.b64decode(requests.get(url).text).decode('utf-8')


def get_a_wiki_from_shard(shard, all_=False):
    gerrit = Gerrit()
    file_ = gerrit.get_file(
        'operations/mediawiki-config/+/master/dblists/{shard}.dblist'.format(
            shard=shard))
    dbs = file_.split('\n')
    random.shuffle(dbs)
    dbs_returning = []
    for line in dbs:
        if not line or line.startswith('#'):
            continue
        dbs_returning.append(line.strip())
        if not all_:
            return [line.strip()]
    return dbs_returning


def get_shard_mapping():
    shard_mapping = {}
    if '--codfw' in sys.argv:
        dc = 'codfw'
    else:
        dc = 'eqiad'
    db_data = requests.get(
        'https://noc.wikimedia.org/dbconfig/{}.json'.format(dc)).json()
    for shard in db_data['sectionLoads']:
        cases = []
        if shard == 'DEFAULT':
            name = 's3'
        else:
            name = shard
        for type_ in db_data['sectionLoads'][shard]:
            cases += list(type_.keys())
        shard_mapping[name] = cases
    return shard_mapping
