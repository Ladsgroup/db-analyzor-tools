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


def get_wikis_from_dblist(dblist, all_=False):
    gerrit = Gerrit()
    file_ = gerrit.get_file(
        'operations/mediawiki-config/+/master/dblists/{dblist}.dblist'.format(
            dblist=dblist))
    dbs = file_.split('\n')
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
        shard_mapping['hosts'][name] = cases
        wikis = get_wikis_from_dblist(name, True)
        for wiki in wikis:
            shard_mapping['wikis'][wiki] = name
    return shard_mapping
