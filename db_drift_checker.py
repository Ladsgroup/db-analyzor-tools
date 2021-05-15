#!/usr/bin/python3

import base64
import json
import random
import re
import sys
import time
from collections import defaultdict
from subprocess import PIPE, TimeoutExpired, run

import requests

# Proudction:
# python3 db_drift_checker.py core "sql {wiki} -- " -prod
# localhost:
# python3 db_drift_checker.py core "sudo mysql -Drepo "


def debug(*args):
    if '-v' in sys.argv or '--verbose' in sys.argv:
        print(*args)


gerrit_url = 'https://gerrit.wikimedia.org/g/'
categories = [
    'core',
    'wikibase_client'
]
with open('abstract_paths.json', 'r') as f:
    type_to_path_mapping_abstracts = json.loads(f.read())

by_db_drifts = {}
by_drift_type_drifts = defaultdict(dict)


class Db():
    def __init__(self, section, host, wiki):
        self.section = section
        self.host = host
        self.wiki = wiki


def get_a_wiki_from_shard(shard, all_=False):
    debug('Getting a wiki from shard:', shard)
    url = gerrit_url + \
        'operations/mediawiki-config/+/master/dblists/{shard}.dblist?format=TEXT'.format(
            shard=shard)
    dbs = base64.b64decode(requests.get(url).text).decode('utf-8').split('\n')
    random.shuffle(dbs)
    dbs_returning = []
    for line in dbs:
        if not line or line.startswith('#'):
            continue
        debug('Got this wiki:', line.strip())
        dbs_returning.append(line.strip())
        if not all_:
            return [line.strip()]
    return dbs_returning


def add_to_drifts(db: Db, table, second, drift_type):
    drift = ' '.join([table, second, drift_type])
    category = sys.argv[1].lower()
    shard = db.section
    if shard not in by_db_drifts:
        by_db_drifts[shard] = defaultdict(list)
    by_db_drifts[shard]['%s:%s' % (db.host, db.wiki)].append(drift)

    if shard not in by_drift_type_drifts[drift]:
        by_drift_type_drifts[drift][shard] = []
    by_drift_type_drifts[drift][shard].append('%s:%s' % (db.host, db.wiki))

    with open('by_db_drifts_{}.json'.format(category), 'w') as f:
        f.write(json.dumps(by_db_drifts, indent=4, sort_keys=True))
    with open('by_drift_type_drifts_{}.json'.format(category), 'w') as f:
        f.write(json.dumps(by_drift_type_drifts, indent=4, sort_keys=True))


def get_file_from_gerrit(url):
    url = gerrit_url + '{0}?format=TEXT'.format(url)
    return base64.b64decode(requests.get(url).text).decode('utf-8')


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


def get_table_structure_sql(host, sql_command, table_name):
    port = None
    if host != 'localhost':
        if re.search(r' \-\-(?: |$)', sql_command):
            sql_command = re.split(r' \-\-(?: |$)', sql_command)[
                0] + ' -- ' + re.split(r' \-\-(?: |$)', sql_command)[1]
        if ':' in host:
            port = host.split(':')[1]
            host = host.split(':')[0]
        if '--codfw' in sys.argv:
            dc = 'codfw'
        else:
            dc = 'eqiad'
        host += '.{}.wmnet'.format(dc)
    debug('Checking table ', table_name)
    if port:
        sql_command += ' -P ' + port
    command = 'timeout 6 ' + sql_command + \
        ' -h %s -e "DESC %s; SHOW INDEX FROM %s;"' % (
            host, table_name, table_name)
    debug('Running:', command)
    try:
        res = run(
            command,
            stdin=PIPE,
            stdout=PIPE,
            shell=True,
            stderr=PIPE,
            timeout=5)
    except TimeoutExpired:
        debug('First timeout has reached')
        try:
            res = run(
                command,
                stdin=PIPE,
                stdout=PIPE,
                shell=True,
                stderr=PIPE,
                timeout=5)
        except BaseException:
            return {}
    if res.stderr and res.stderr.decode('utf-8'):
        return {}
    result = res.stdout.decode('utf-8').split('\nTable\t')
    if len(result) != 2:
        print(result)
        raise Exception
    return (result[0].split('\n'), result[1].split('\n')[1:])


def build_expected(expected_column):
    expected = {}
    expected['type'] = expected_column['type'].replace(
        'string', 'binary').replace('integer', 'int')
    expected['size'] = expected_column['options'].get('length', 0)

    if expected['type'] == 'binary' and not expected_column['options'].get(
            'fixed'):
        expected['type'] = 'varbinary'
    elif expected['type'] in ('blob', 'text'):
        if expected['size'] < 256:
            expected['type'] = 'tinyblob'
        elif expected['size'] < 65536:
            expected['type'] = 'blob'
        elif expected['size'] < 16777216:
            expected['type'] = 'mediumblob'
    elif expected['type'] == 'mwtinyint':
        expected['type'] = 'tinyint'
    elif expected['type'] == 'mwenum':
        expected['type'] = 'enum'
        expected['size'] = set([
            i.lower() for i in expected_column['options'].get(
                'CustomSchemaOptions', {}).get(
                'enum_values', [])])
    elif expected['type'] == 'mwtimestamp':
        if expected_column['options'].get(
                'CustomSchemaOptions', {}).get(
                'allowInfinite', False):
            expected['type'] = 'varbinary'
        else:
            expected['type'] = 'binary'
        expected['size'] = 14
    elif expected['type'] in ('datetimetz'):
        expected['type'] = 'timestamp'
    expected['not_null'] = expected_column['options'].get('notnull', False)
    expected['unsigned'] = expected_column['options'].get('unsigned', False)
    expected['auto_increment'] = expected_column['options'].get(
        'autoincrement', False)

    return expected


def handle_column(expected, field_structure, db, table_name, name):
    actual_size = None
    if '(' in field_structure[1]:
        actual_size = field_structure[1].split('(')[1].split(')')[0]
        if ',' in actual_size:
            actual_size = set(actual_size.replace(
                '\'', '').replace('"', '').split(','))

    if actual_size and expected['size']:
        if not isinstance(expected['size'], set):
            the_same = int(actual_size) == int(expected['size'])
        else:
            the_same = actual_size == expected['size']
        if not the_same:
            add_to_drifts(db, table_name, name, 'field-size-mismatch')
    if (expected['unsigned'] and ' unsigned' not in field_structure[1]) or (
            expected['unsigned'] is False and ' unsigned' in field_structure[1]):
        add_to_drifts(db, table_name, name, 'field-unsigned-mismatch')
    actual_type = field_structure[1].split('(')[0].split(' ')[0]
    actual_type = actual_type.replace(
        'double precision', 'float').replace(
        'double', 'float')
    if actual_type != expected['type']:
        add_to_drifts(db, table_name, name, 'field-type-mismatch')
    if (field_structure[2] == 'no' and expected['not_null'] is not True) or (
            field_structure[2] == 'yes' and expected['not_null'] is not False):
        add_to_drifts(db, table_name, name, 'field-nullable-mismatch')


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
            add_to_drifts(db, table_name, name, 'field-mismatch-prod-extra')
            continue

        expected = build_expected(expected_column)
        handle_column(
            expected,
            field_structure,
            db,
            table_name,
            column['name'])

    for column in expected_table['columns']:
        if column['name'] not in fields_in_prod:
            add_to_drifts(
                db,
                table_name,
                column['name'],
                'field-mismatch-codebase-extra')

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
        if index == 'primary':
            if indexes[index]['columns'] != expected_pk:
                add_to_drifts(db, table_name, index, 'primary-key-mismatch')
            continue
        expected_index = None
        for i in expected_indexes:
            if i['name'] == index:
                expected_index = i
                break
        else:
            add_to_drifts(db, table_name, index, 'index-mismatch-prod-extra')
            continue
        if indexes[index]['unique'] != expected_index['unique']:
            add_to_drifts(db, table_name, index, 'index-uniqueness-mismatch')
        expected_columns = expected_index['columns']
        if indexes[index]['columns'] != expected_columns:
            add_to_drifts(db, table_name, index, 'index-columns-mismatch')

    for index in expected_indexes:
        if index['name'] not in indexes:
            add_to_drifts(
                db,
                table_name,
                index['name'],
                'index-mismatch-code-extra')


def handle_wiki(shard, sql_data, hosts, wiki, sql_command):
    if shard is not None:
        sql_command = sql_command.format(wiki=wiki)
        debug('Sql command for this shard', sql_command)
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
    with open('by_db_drifts_{}.json'.format(category), 'w') as f:
        f.write(json.dumps({}))
    with open('by_drift_type_drifts_{}.json'.format(category), 'w') as f:
        f.write(json.dumps({}))
    shard_mapping = get_shard_mapping()
    if category in type_to_path_mapping_abstracts:
        sql_data = []
        for path in type_to_path_mapping_abstracts[category]:
            sql_data += json.loads(get_file_from_gerrit(path))
    else:
        raise Exception
    if '-prod' in sys.argv:
        for shard in shard_mapping:
            handle_shard(shard, sql_data, shard_mapping[shard], False)
    else:
        handle_shard(None, sql_data, ['localhost'])


def main():
    category = sys.argv[1].lower()
    if category == 'all':
        for cat in categories:
            handle_category(cat)
    else:
        handle_category(category)


main()
