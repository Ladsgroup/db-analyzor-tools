from domain.db import Db


class Checker(object):
    def __init__(self, db: Db, table_name, piece_name, drifts):
        self.db = db
        self.table_name = table_name
        self.piece_name = piece_name
        # Shared in-memory dict, written to disk once at the end of the run
        self.drifts = drifts

    def run_check(self, drift_type, check):
        if not check:
            return
        self._report(drift_type)

    def _report(self, drift_type):
        drift = ' '.join([self.table_name, self.piece_name, drift_type])
        self.drifts.setdefault(drift, {}).setdefault(self.db.section, []).append(
            '%s:%s' % (self.db.host, self.db.wiki))


class CheckerFactory():
    def __init__(self, db: Db, drifts, table_name):
        self.db = db
        self.drifts = drifts
        self.table_name = table_name

    def get_checker(self, piece_name):
        return Checker(self.db, self.table_name, piece_name, self.drifts)
