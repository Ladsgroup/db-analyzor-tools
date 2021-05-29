#!/usr/bin/python3

import json
import sys
import time

from checker import Checker
from data_access.sql import get_table_structure_sql
from data_access.wmf import get_a_wiki_from_shard, get_shard_mapping, Gerrit
from domain.table import Column
from domain.db import Db

# Proudction:
# python3 db_drift_checker.py core "sql {wiki} -- " -prod
# localhost:
# python3 db_drift_checker.py core "sudo mysql -Drepo "

categories = [
    'core',
    'wikibase_client'
]
with open('abstract_paths.json', 'r') as f:
    type_to_path_mapping_abstracts = json.loads(f.read())


def handle_column(expected: Column, field_structure, checker: Checker):
    actual_size = None
    if '(' in field_structure[1]:
        actual_size = field_structure[1].split('(')[1].split(')')[0]
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
        (expected.unsigned and ' unsigned' not in field_structure[1]) or \
        (expected.unsigned is False and ' unsigned' in field_structure[1])
    checker.run_check('field-unsigned-mismatch', unsigned_mismatch)
    actual_type = field_structure[1].split('(')[0].split(' ')[0]
    actual_type = actual_type.replace(
        'double precision', 'float').replace(
        'double', 'float')
    checker.run_check('field-type-mismatch', actual_type != expected.type_)
    nullable_mismatch = \
        (field_structure[2] == 'no' and expected.not_null is not True) or \
        (field_structure[2] == 'yes' and expected.not_null is not False)
    checker.run_check('field-nullable-mismatch', nullable_mismatch)


def compare_table_with_prod(db, expected_table, sql_command):
    actual_table = get_table_structure_sql(
        db.host, sql_command, expected_table['name'])
    if not actual_table:
        print('no response')
        return {}
    table_sql = actual_table[0]
    table_indexes = actual_table[1]
    if not table_sql or not table_indexes:
        print('no response')
        return {}
    fields_in_prod = []
    table_name = expected_table['name']
    for line in table_sql:
        if line.startswith('ERROR'):
            return
        if not line or line.startswith('Field'):
            continue
        field_structure = line.lower().split('\t')
        fields_in_prod.append(field_structure[0])
        name = field_structure[0]
        expected_column = None
        for column in expected_table['columns']:
            if column['name'] == name:
                expected_column = column
                break
        else:
            checker = Checker(db, table_name, name)
            checker.run_check('field-mismatch-prod-extra', True)
            continue

        expected = Column.newFromAbstractSchema(expected_column)
        checker = Checker(db, table_name, column['name'])
        handle_column(expected, field_structure, checker)

    for column in expected_table['columns']:
        checker = Checker(db, table_name, column['name'])
        checker.run_check(
            'field-mismatch-codebase-extra',
            column['name'] not in fields_in_prod)

    indexes = {}
    for line in table_indexes:
        if line.startswith('ERROR'):
            return
        if not line or line.startswith('Table'):
            continue
        index_structure = line.lower().split('\t')

        if index_structure[2] not in indexes:
            indexes[index_structure[2]] = {
                'unique': index_structure[1] == '0',
                'columns': [index_structure[4]]
            }
        else:
            indexes[index_structure[2]]['columns'].append(index_structure[4])
    expected_indexes = expected_table['indexes']
    expected_pk = expected_table.get('pk')
    for index in indexes:
        checker = Checker(db, table_name, index)
        if index == 'primary':
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
        checker = Checker(db, table_name, index['name'])
        checker.run_check(
            'index-mismatch-code-extra',
            index['name'] not in indexes)


def handle_wiki(shard, sql_data, hosts, wiki, sql_command):
    if shard is not None:
        sql_command = sql_command.format(wiki=wiki)
    for host in hosts:
        for table in sql_data:
            if table['name'] == 'searchindex':
                continue
            print(host, table['name'])
            db = Db(shard, host, wiki)
            compare_table_with_prod(db, table, sql_command)
            time.sleep(1)


def handle_shard(shard, sql_data, hosts, all_=False):
    sql_command = sys.argv[2]
    if shard is not None:
        wikis = get_a_wiki_from_shard(shard, all_)
    else:
        wikis = ['']
    for wiki in wikis:
        handle_wiki(shard, sql_data, hosts, wiki, sql_command)


def handle_category(category):
    with open('drifts_{}.json'.format(category), 'w') as f:
        f.write(json.dumps({
        '_metadata': {
            'time_start': time.time()
        }
    }))
    shard_mapping = get_shard_mapping()

    if category in type_to_path_mapping_abstracts:
        sql_data = []
        gerrit = Gerrit()
        for path in type_to_path_mapping_abstracts[category]:
            sql_data += json.loads(gerrit.get_file(path))
    else:
        raise Exception
    if '-prod' in sys.argv:
        for shard in shard_mapping:
            handle_shard(shard, sql_data, shard_mapping[shard], False)
    else:
        handle_shard(None, sql_data, ['localhost'])

    with open('drifts_{}.json'.format(category), 'r') as f:
        drifts = json.loads(f.read())
    drifts['_metadata']['time_end'] = time.time()
    with open('drifts_{}.json'.format(category), 'w') as f:
        f.write(json.dumps(drifts, indent=4, sort_keys=True))


def main():
    category = sys.argv[1].lower()
    if category == 'all':
        for cat in categories:
            handle_category(cat)
    else:
        handle_category(category)


main()
