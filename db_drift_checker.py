#!/usr/bin/python3

import argparse
import json
import re
import time
from collections import defaultdict

from checker import Checker, CheckerFactory
from data_access.sql import get_table_structure_sql
from data_access.wmf import Gerrit, get_wikis_from_dblist, get_shard_mapping
from domain.db import Db
from domain.table import Column

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument(
    'type',
    help='Type of check, core, wikibase_client, etc.')
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
    '--all', action='store_true',
    help='All wikis in sections')
parser.add_argument(
    '--dc', default='eqiad',
    help='All wikis in sections')

args = parser.parse_args()

categories = [
    'core',
    'wikibase_client'
]
with open('abstract_paths.json', 'r') as f:
    schema_config = json.loads(f.read())


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


def compare_table_with_prod(db, expected_table, actual_table):
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
    checker_factory = CheckerFactory(db, args.type, table_name)
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

    for column in expected_table['columns']:
        checker = checker_factory.get_checker(actual_column['COLUMN_NAME'])
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


def handle_wiki(shard, sql_data, hosts, wiki, sql_command):
    if shard is not None:
        sql_command = sql_command.format(wiki=wiki)
    for host in hosts:
        db = Db(shard, host, wiki)
        if args.wiki:
            wiki = args.wiki
        print(wiki, host)
        res = get_table_structure_sql(host, sql_command, wiki, args.dc)
        data_ = defaultdict(list)
        for row in res.split('\n******'):
            def_ = re.findall(
                r'^\s*([A-Z_]+?)\s*: *(.*?) *$',
                '\n'.join(
                    row.split('\n')[
                        1:]),
                re.M)
            if not def_:
                continue
            def_ = dict(def_)
            data_[def_['TABLE_NAME']].append(def_)
        for table in sql_data:
            if table['name'] == 'searchindex':
                continue
            if table['name'] not in data_:
                continue
            compare_table_with_prod(db, table, data_[table['name']])


def handle_dblist(dblist, sql_data, shard_mapping, all_=False):
    if dblist is not None:
        wikis = get_wikis_from_dblist(dblist, all_)
    else:
        wikis = ['']
    for wiki in wikis:
        shard = shard_mapping['wikis'][wiki]
        handle_wiki(dblist, sql_data, shard_mapping['hosts'][shard], wiki, args.command)


def handle_category(category):
    with open('drifts_{}.json'.format(category), 'w') as f:
        f.write(json.dumps({
            '_metadata': {
                'time_start': time.time()
            }
        }))
    shard_mapping = get_shard_mapping(args.dc)

    if category in schema_config:
        sql_data = []
        gerrit = Gerrit()
        for path in schema_config[category]['path']:
            sql_data += json.loads(gerrit.get_file(path))
    else:
        raise Exception
    if args.prod:
        if schema_config[category].get('dblist'):
            handle_dblist(schema_config[category]['dblist'], sql_data, shard_mapping, args.all)
        else:
            for shard in shard_mapping:
                handle_dblist(shard, sql_data, shard_mapping, args.all)
    else:
        # supporting localhost is fun
        handle_dblist(None, sql_data, {'hosts': {'': ['localhost']}, 'wikis': {'': ''}})

    with open('drifts_{}.json'.format(category), 'r') as f:
        drifts = json.loads(f.read())
    drifts['_metadata']['time_end'] = time.time()
    with open('drifts_{}.json'.format(category), 'w') as f:
        f.write(json.dumps(drifts, indent=4, sort_keys=True))


def main():
    category = args.type.lower()
    if category == 'all':
        for cat in categories:
            handle_category(cat)
    else:
        handle_category(category)


main()
