import json
import os
import tempfile
import unittest
from unittest import mock

import db_drift_checker

FIXTURE = os.path.join(os.path.dirname(__file__), 'fixture_information_schema.txt')


def col(name, type_, **options):
    return {'name': name, 'type': type_, 'options': options}


SCHEMA = [
    {
        'name': 'foo',
        'columns': [
            col('id', 'integer', notnull=True, unsigned=True, autoincrement=True),
            col('foo_name', 'binary', notnull=True, length=255),
            col('foo_flag', 'mwtinyint', notnull=True, unsigned=True),
            col('foo_code_only', 'blob', length=255),
        ],
        'indexes': [
            {'name': 'foo_name', 'columns': ['foo_name'], 'unique': False},
            {'name': 'foo_code_idx', 'columns': ['foo_flag'], 'unique': False},
        ],
        'pk': ['id'],
    },
    {
        'name': 'bar',
        'columns': [col('bar_id', 'integer', notnull=True)],
        'indexes': [],
        'pk': ['bar_id'],
    },
    # Not in the database, must be ignored
    {'name': 'baz', 'columns': [col('baz_id', 'integer')], 'indexes': []},
]

EXPECTED_DRIFTS = {
    'foo foo_name field-size-mismatch',
    'foo foo_flag field-nullable-mismatch',
    'foo foo_prod_only field-mismatch-prod-extra',
    'foo foo_code_only field-mismatch-codebase-extra',
    'foo foo_prod_idx index-mismatch-prod-extra',
    'foo foo_code_idx index-mismatch-code-extra',
}


class TestLocalRun(unittest.TestCase):
    """Runs the whole checker against a fake mysql client."""

    def setUp(self):
        self.cwd = os.getcwd()
        self.tmp = tempfile.TemporaryDirectory()
        os.chdir(self.tmp.name)

    def tearDown(self):
        os.chdir(self.cwd)
        self.tmp.cleanup()

    def run_checker(self, argv, schema_files):
        # `true` swallows the mysql arguments appended by the checker
        command = 'cat {}; true '.format(FIXTURE)
        with mock.patch('db_drift_checker.Gerrit.get_file', side_effect=schema_files.get), \
                mock.patch('db_drift_checker.Gerrit.get_url', side_effect=schema_files.get), \
                mock.patch('db_drift_checker.get_table_catalog', return_value=CATALOG):
            db_drift_checker.main(argv[:1] + [command] + argv[1:])

    def read(self, name):
        with open(name) as f:
            return json.load(f)

    def test_custom(self):
        self.run_checker(
            ['custom', '--wiki', 'wiki', '--gerrit-schema-file', 'x/tables.json'],
            {'x/tables.json': json.dumps(SCHEMA)})
        drifts = self.read('drifts_custom.json')
        metadata = drifts.pop('_metadata')
        self.assertLessEqual(metadata['time_start'], metadata['time_end'])
        self.assertEqual(set(drifts), EXPECTED_DRIFTS)
        for drift in drifts.values():
            self.assertEqual(drift, {'': ['localhost:wiki']})

    def test_catalog_source(self):
        self.run_checker(
            ['src_a', '--wiki', 'wiki'],
            {'https://gerrit/a.json': json.dumps(SCHEMA)})
        drifts = self.read('drifts_src_a.json')
        drifts.pop('_metadata')
        self.assertEqual(set(drifts), EXPECTED_DRIFTS)
        self.assertFalse(os.path.exists('drifts_src_b.json'))

    def test_all_sources_split_by_source(self):
        self.run_checker(
            ['all', '--wiki', 'wiki'],
            {'https://gerrit/a.json': json.dumps(SCHEMA[:1]),
             'https://gerrit/b.json': json.dumps(SCHEMA[1:])})
        a = self.read('drifts_src_a.json')
        b = self.read('drifts_src_b.json')
        a.pop('_metadata')
        b.pop('_metadata')
        self.assertEqual(set(a), EXPECTED_DRIFTS)
        self.assertEqual(b, {})

    def test_dbprefix(self):
        prefixed = [dict(t, name='foo') for t in SCHEMA[:1]]
        self.run_checker(
            ['custom', '--wiki', 'wiki', '--dbprefix', 'fo', '--gerrit-schema-file', 'x'],
            {'x': json.dumps([dict(prefixed[0], name='o')])})
        drifts = self.read('drifts_custom.json')
        drifts.pop('_metadata')
        self.assertEqual(
            set(drifts), {d.replace('foo ', 'o ', 1) for d in EXPECTED_DRIFTS})

    def test_unknown_type(self):
        with self.assertRaises(Exception):
            self.run_checker(['nope'], {})

    def test_custom_requires_schema_file(self):
        with self.assertRaises(Exception):
            self.run_checker(['custom'], {})
        with self.assertRaises(Exception):
            self.run_checker(['core', '--gerrit-schema-file', 'x'], {})


CATALOG = {
    'sources': {
        'src_a': [{'gerrit': 'https://gerrit/a.json'}],
        'src_b': [{'gerrit': 'https://gerrit/b.json'}],
    },
    'tables': [
        {'name': 'everywhere', 'source': 'src_a'},
        {'name': 'on_list', 'source': 'src_a', 'dblist': 'special'},
        {'name': 'shared', 'source': 'src_a',
         'databases': [{'dbname': 'wikishared', 'cluster': 'x1'}, {'dbname': 'aawiki'}]},
        {'name': 'on_x1_list', 'source': 'src_a',
         'databases': [{'dblist': 'special', 'cluster': 'x1'}]},
        {'name': 'bad_cluster', 'source': 'src_a',
         'databases': [{'dbname': 'aawiki', 'cluster': 'x99'}]},
    ],
}

SHARD_MAPPING = {
    'hosts': {'s1': ['db1', 'db2'], 's3': ['db3'], 'x1': ['dbx']},
    'wikis': {'enwiki': 's1', 'aawiki': 's3', 'bbwiki': 's3'},
}

DBLISTS = {'s1': ['enwiki'], 's3': ['aawiki', 'bbwiki'], 'special': ['bbwiki']}


def fake_dblist(name, all_=False):
    if name == 's3' and not all_:
        return DBLISTS[name][:1]
    return DBLISTS[name]


class TestTargets(unittest.TestCase):
    def targets(self, names, *argv):
        db_drift_checker.args = db_drift_checker.parser.parse_args(['core', 'cmd'] + list(argv))
        with mock.patch('db_drift_checker.get_wikis_from_dblist', side_effect=fake_dblist):
            return {k: sorted(v) for k, v in
                    db_drift_checker.get_targets(names, CATALOG, SHARD_MAPPING).items()}

    def test_placement(self):
        self.assertEqual(
            self.targets(['everywhere', 'on_list', 'shared', 'on_x1_list',
                          'bad_cluster', 'not_in_catalog'], '--all'),
            {
                ('s1', 'enwiki'): ['everywhere', 'not_in_catalog'],
                ('s3', 'aawiki'): ['everywhere', 'not_in_catalog', 'shared'],
                ('s3', 'bbwiki'): ['everywhere', 'not_in_catalog', 'on_list'],
                ('x1', 'wikishared'): ['shared'],
                ('x1', 'bbwiki'): ['on_x1_list'],
            })

    def test_s3_sampled_without_all(self):
        self.assertEqual(
            self.targets(['everywhere', 'on_list']),
            {
                ('s1', 'enwiki'): ['everywhere'],
                ('s3', 'aawiki'): ['everywhere'],
                ('s3', 'bbwiki'): ['on_list'],
            })


class TestProdRun(unittest.TestCase):
    """Every (host, wiki) pair is queried once, with all of its tables."""

    def test_queries(self):
        calls = []

        def fake_sql(host, sql_command, db, dc, skip_host):
            calls.append((host, sql_command, db))
            with open(FIXTURE) as f:
                return f.read()

        cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmp:
            os.chdir(tmp)
            try:
                with mock.patch('db_drift_checker.Gerrit.get_url',
                                return_value=json.dumps(SCHEMA)), \
                        mock.patch('db_drift_checker.get_table_catalog', return_value=CATALOG), \
                        mock.patch('db_drift_checker.get_shard_mapping', return_value=SHARD_MAPPING), \
                        mock.patch('db_drift_checker.get_wikis_from_dblist', side_effect=fake_dblist), \
                        mock.patch('db_drift_checker.get_table_structure_sql', side_effect=fake_sql):
                    db_drift_checker.main(['src_a', 'sql {wiki} --', '--prod', '--all'])
                with open('drifts_src_a.json') as f:
                    drifts = json.load(f)
            finally:
                os.chdir(cwd)

        self.assertEqual(sorted(calls), [
            ('db1', 'sql enwiki --', 'enwiki'),
            ('db2', 'sql enwiki --', 'enwiki'),
            ('db3', 'sql aawiki --', 'aawiki'),
            ('db3', 'sql bbwiki --', 'bbwiki'),
        ])
        self.assertEqual(
            drifts['foo foo_name field-size-mismatch'],
            {'s1': ['db1:enwiki', 'db2:enwiki'], 's3': ['db3:aawiki', 'db3:bbwiki']})


if __name__ == '__main__':
    unittest.main()
