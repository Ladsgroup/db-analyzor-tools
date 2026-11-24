#!/usr/bin/python3

import argparse
import json
import os
import re
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import requests

from checker import Checker, CheckerFactory
from data_access.sql import get_table_structure_sql
from data_access.wmf import (Gerrit, get_shard_mapping, get_table_catalog,
                             get_wikis_from_dblist)
from domain.db import Db
from domain.table import Column

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument(
    'type',
    help='Source of tables in the table catalog (core, wikibase_client, etc.), '
         '"all" for every source, or "custom" with --gerrit-schema-file')
parser.add_argument(
    'command',
    help='Command to get to sql, for production it should be something like "sql {wiki} -- " and "sudo mysql " for localhost')
parser.add_argument(
    '--prod', action='store_true',
    help='Whether it is happening in production or not')

parser.add_argument(
    '--wiki',
    help='Wiki db name, for use in localhost')
parser.add_argument(
    '--dbprefix',
    help='Prefix for database tables'
)
parser.add_argument(
    '--all', action='store_true',
    help='All wikis in sections')
parser.add_argument(
    '--dc', default='eqiad',
    help='All wikis in sections')
parser.add_argument(
    '--skip-host', action='store_true',
    help='Skip the addition of the host parameter to the sql command invocation'
)
parser.add_argument(
    '--gerrit-schema-file',
    help='Custom abstract schema file to read from, like `mediawiki/core/+/REL1_43/maintenance/tables.json`'
)
parser.add_argument(
    '--jobs', type=int, default=8,
    help='Number of database queries to run in parallel'
)
parser.add_argument(
    '--output-dir', default='.',
    help='Directory to write drifts_<source>.json files to, '
         'drift-tracker reads them from drifts/ in this repository'
)

args = None


def handle_column(expected: Column, column_type, nullable, checker: Checker):
    actual_size = None
    if '(' in column_type:
        actual_size = column_type.split('(')[1].split(')')[0]
        if ',' in actual_size:
            actual_size = set(actual_size.replace(
                '\'', '').replace('"', '').split(','))

    if actual_size and expected.size_:
        if not isinstance(expected.size_, set):
            the_same = int(actual_size) == int(expected.size_)
        else:
            the_same = actual_size == expected.size_
        checker.run_check('field-size-mismatch', not the_same)
    unsigned_mismatch = \
        (expected.unsigned and ' unsigned' not in column_type) or \
        (expected.unsigned is False and ' unsigned' in column_type)
    checker.run_check('field-unsigned-mismatch', unsigned_mismatch)
    actual_type = column_type.split('(')[0].split(' ')[0]
    actual_type = actual_type.replace(
        'double precision', 'float').replace(
        'double', 'float')
    checker.run_check('field-type-mismatch', actual_type != expected.type_)
    nullable_mismatch = \
        (nullable == 'no' and expected.not_null is not True) or \
        (nullable == 'yes' and expected.not_null is not False)
    checker.run_check('field-nullable-mismatch', nullable_mismatch)


def compare_table_with_prod(db, expected_table, actual_table, drifts):
    if not actual_table:
        print('no response')
        return {}
    table_sql = [i for i in actual_table if 'COLUMN_COMMENT' in i]
    table_indexes = [i for i in actual_table if 'INDEX_NAME' in i]
    if not table_sql or not table_indexes:
        print('no response')
        return {}
    fields_in_prod = []
    table_name = expected_table['name']
    checker_factory = CheckerFactory(db, drifts, table_name)
    for actual_column in table_sql:
        fields_in_prod.append(actual_column['COLUMN_NAME'])
        name = actual_column['COLUMN_NAME']
        expected_column = None
        for column in expected_table['columns']:
            if column['name'] == name:
                expected_column = column
                break
        else:
            checker = checker_factory.get_checker(name)
            checker.run_check('field-mismatch-prod-extra', True)
            continue

        expected = Column.newFromAbstractSchema(expected_column)
        checker = checker_factory.get_checker(actual_column['COLUMN_NAME'])
        handle_column(
            expected,
            actual_column['COLUMN_TYPE'],
            actual_column['IS_NULLABLE'].lower(),
            checker)

        checker.run_check(
            'field-auto-increment-mismatch',
            ( 'auto_increment' in actual_column['EXTRA'] ) != expected.auto_increment
        )

    for column in expected_table['columns']:
        checker = checker_factory.get_checker(column['name'])
        checker.run_check(
            'field-mismatch-codebase-extra',
            column['name'] not in fields_in_prod)

    indexes = {}
    for actual_index in table_indexes:
        if actual_index['INDEX_NAME'] not in indexes:
            indexes[actual_index['INDEX_NAME']] = {
                'unique': actual_index['NON_UNIQUE'] == '0',
                'columns': [actual_index['COLUMN_NAME']]
            }
        else:
            indexes[actual_index['INDEX_NAME']]['columns'].append(
                actual_index['COLUMN_NAME'])
    expected_indexes = expected_table['indexes']
    expected_pk = expected_table.get('pk')
    for index in indexes:
        checker = checker_factory.get_checker(index)
        if index == 'PRIMARY':
            checker.run_check(
                'primary-key-mismatch',
                indexes[index]['columns'] != expected_pk
            )
            continue
        expected_index = None
        for i in expected_indexes:
            if i['name'] == index:
                expected_index = i
                break
        else:
            checker.run_check('index-mismatch-prod-extra', True)
            continue
        checker.run_check(
            'index-uniqueness-mismatch',
            indexes[index]['unique'] != expected_index['unique']
        )
        expected_columns = expected_index['columns']
        checker.run_check(
            'index-columns-mismatch',
            indexes[index]['columns'] != expected_columns
        )

    for index in expected_indexes:
        checker = checker_factory.get_checker(index['name'])
        checker.run_check(
            'index-mismatch-code-extra',
            index['name'] not in indexes)


def parse_sql_output(res):
    """Group the information_schema rows by table name."""
    data_ = defaultdict(list)
    for row in res.split('\n******'):
        def_ = re.findall(
            r'^\s*([A-Z_]+?)\s*: *(.*?) *$',
            '\n'.join(row.split('\n')[1:]),
            re.M)
        if not def_:
            continue
        def_ = dict(def_)
        if args.dbprefix:
            # Only use tables that have the prefix, to avoid overwriting
            # data with unrelated tables
            if not def_['TABLE_NAME'].startswith(args.dbprefix):
                continue
            def_['TABLE_NAME'] = def_['TABLE_NAME'].removeprefix(args.dbprefix)
        data_[def_['TABLE_NAME']].append(def_)
    return data_


def get_expected_tables(category, catalog):
    """Map of table name to (source, abstract schema of the table)."""
    if category == 'custom':
        if not args.gerrit_schema_file:
            raise Exception("'custom' type requires --gerrit-schema-file to be provided")
        schema = json.loads(Gerrit().get_file(args.gerrit_schema_file))
        return {table['name']: ('custom', table) for table in schema}
    if args.gerrit_schema_file:
        raise Exception("--gerrit-schema-file should only be used with the 'custom' type")
    if category == 'all':
        sources = catalog['sources']
    elif category in catalog['sources']:
        sources = {category: catalog['sources'][category]}
    else:
        raise Exception(
            "Unsupported type %s, use a source from the table catalog or type 'custom' "
            "and --gerrit-schema-file" % category)
    files = [(source, f['gerrit']) for source, files in sources.items() for f in files]
    def get_schema(url):
        try:
            return json.loads(Gerrit().get_url(url))
        except requests.HTTPError as e:
            # The catalog can point to files that are moved or removed
            print('Skipping schema file: {}'.format(e), file=sys.stderr)
            return []

    with ThreadPoolExecutor(args.jobs) as pool:
        schemas = pool.map(get_schema, [url for _, url in files])
    tables = {}
    for (source, _), schema in zip(files, schemas):
        for table in schema:
            if table['name'] != 'searchindex':
                tables[table['name']] = (source, table)
    return tables


def get_targets(table_names, catalog, shard_mapping):
    """Map of (section, wiki) to the table names expected there.

    Placement comes from the table catalog. Tables without placement
    information are expected on every wiki.
    """
    placements = {t['name']: t for t in catalog['tables']}
    every_wiki = []
    for shard in shard_mapping['wikis'].values():
        if shard not in every_wiki:
            every_wiki.append(shard)
    # Resolve once, so that the random sample of s3 is the same for all tables
    every_wiki = [w for shard in every_wiki for w in get_wikis_from_dblist(shard, args.all)]

    targets = defaultdict(set)
    for name in table_names:
        placement = placements.get(name, {})
        if placement.get('databases'):
            locations = placement['databases']
        elif placement.get('dblist'):
            locations = [{'dblist': placement['dblist']}]
        else:
            locations = [{}]
        for location in locations:
            if 'dbname' in location:
                wikis = [location['dbname']]
            elif 'dblist' in location:
                wikis = get_wikis_from_dblist(location['dblist'], True)
            else:
                wikis = every_wiki
            for wiki in wikis:
                section = location.get('cluster') or shard_mapping['wikis'].get(wiki)
                if section not in shard_mapping['hosts']:
                    print('Skipping {} on {}: unknown section {}'.format(
                        name, wiki, section), file=sys.stderr)
                    continue
                targets[(section, wiki)].add(name)
    return targets


def fetch(job):
    section, host, wiki, _ = job
    sql_command = args.command.format(wiki=wiki)
    return get_table_structure_sql(host, sql_command, wiki, args.dc, args.skip_host)


def run_checks(category):
    time_start = time.time()
    catalog = get_table_catalog() if category != 'custom' or args.prod else None
    expected = get_expected_tables(category, catalog)
    if args.prod:
        shard_mapping = get_shard_mapping(args.dc)
        targets = get_targets(expected, catalog, shard_mapping)
    else:
        # supporting localhost is fun
        shard_mapping = {'hosts': {'': ['localhost']}}
        targets = {('', args.wiki or ''): set(expected)}

    jobs = [
        (section, host, wiki, sorted(names))
        for (section, wiki), names in sorted(targets.items())
        for host in shard_mapping['hosts'][section]
    ]
    drifts = {source: {} for source, _ in expected.values()}
    # Queries run in parallel, comparisons run in order in this thread
    with ThreadPoolExecutor(args.jobs) as pool:
        for job, res in zip(jobs, pool.map(fetch, jobs)):
            section, host, wiki, names = job
            print(wiki, host)
            db = Db(section, host, wiki)
            data_ = parse_sql_output(res)
            for name in names:
                if name not in data_:
                    continue
                source, table = expected[name]
                compare_table_with_prod(db, table, data_[name], drifts[source])

    for source, source_drifts in drifts.items():
        source_drifts['_metadata'] = {
            'time_start': time_start,
            'time_end': time.time()
        }
        path = os.path.join(args.output_dir, 'drifts_{}.json'.format(source))
        with open(path, 'w') as f:
            f.write(json.dumps(source_drifts, indent=4, sort_keys=True))


def main(argv=None):
    global args
    args = parser.parse_args(argv)
    run_checks(args.type.lower())


if __name__ == '__main__':
    main()
