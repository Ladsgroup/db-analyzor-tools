
import sys
import json

from domain.db import Db


class Checker(object):
    def __init__(self, db: Db, table_name, piece_name):
        self.db = db
        self.table_name = table_name
        self.piece_name = piece_name

    def run_check(self, drift_type, check):
        if not check:
            return
        self._report(drift_type)

    def _report(self, drift_type):
        drift = ' '.join([self.table_name, self.piece_name, drift_type])
        category = sys.argv[1].lower()
        shard = self.db.section
        with open('drifts_{}.json'.format(category), 'r') as f:
            drifts = json.loads(f.read())

        if drift not in drifts:
            drifts[drift] = {}
        if shard not in drifts[drift]:
            drifts[drift][shard] = []
        drifts[drift][shard].append('%s:%s' % (self.db.host, self.db.wiki))

        with open('drifts_{}.json'.format(category), 'w') as f:
            f.write(json.dumps(drifts, indent=4, sort_keys=True))
