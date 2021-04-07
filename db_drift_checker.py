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

# Something like this: python3 new_db_checker.py core "sql {wiki} -- " s8 --important-only -prod
# for localhost: python3 new_db_checker.py core "sudo mysql -Drepo " s8
# --important-only -v


def debug(*args):
    if '-v' in sys.argv or '--verbose' in sys.argv:
        print(*args)


gerrit_url = 'https://gerrit.wikimedia.org/g/'
type_to_path_mapping = {
    'core': 'mediawiki/core/+/master/maintenance/tables.sql',
    'wikibase-repo': 'mediawiki/extensions/Wikibase/+/master/repo/sql/Wikibase.sql',
    'wikibase-client': 'mediawiki/extensions/Wikibase/+/master/client/sql/entity_usage.sql',
    'abusefilter': 'mediawiki/extensions/AbuseFilter/+/master/abusefilter.tables.sqlite.sql',
    'flaggedrevs': 'mediawiki/extensions/FlaggedRevs/+/master/backend/schema/mysql/FlaggedRevs.sql',
}
type_to_path_mapping_abstracts = {
    'core': 'mediawiki/core/+/master/maintenance/tables.json',
    'patch': 'mediawiki/core/+/refs/changes/55/619155/5/maintenance/tables.json'
}
by_db_drifts = {}
by_drift_type_drifts = defaultdict(dict)


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


def add_to_drifts(shard, db, table, second, drift_type, wiki):
    drift = ' '.join([table, second, drift_type])
    if shard not in by_db_drifts:
        by_db_drifts[shard] = defaultdict(list)
    by_db_drifts[shard]['%s:%s' % (db, wiki)].append(drift)

    if shard not in by_drift_type_drifts[drift]:
        by_drift_type_drifts[drift][shard] = []
    by_drift_type_drifts[drift][shard].append('%s:%s' % (db, wiki))

    with open('by_db_drifts.json', 'w') as f:
        f.write(json.dumps(by_db_drifts, indent=4, sort_keys=True))
    with open('by_drift_type_drifts.json', 'w') as f:
        f.write(json.dumps(by_drift_type_drifts, indent=4, sort_keys=True))


def get_sql_from_gerrit(url):
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


# https://stackoverflow.com/questions/5903720/recursive-diff-of-two-python-dictionaries-keys-and-values
def dd(d1, d2, ctx=""):
    for k in d1:
        if k not in d2:
            print(k + " removed from " + ctx)
    for k in d2:
        if k not in d1:
            print(k + " added in " + ctx)
            continue
        if d2[k] != d1[k]:
            if type(d2[k]) not in (dict, list):
                print(k + " changed in " + ctx + " to " + str(d2[k]))
            else:
                if not isinstance(d1[k], type(d2[k])):
                    print(k + " changed in " + ctx + " to " + str(d2[k]))
                    continue
                else:
                    if isinstance(d2[k], dict):
                        dd(d1[k], d2[k], k + ' in ' + ctx)
                        continue
    return


def parse_sql(sql):
    result = []
    sql = sql.replace('IF NOT EXISTS ', '')
    for table_chunk in sql.split('CREATE TABLE '):
        table_chunk = table_chunk.lower()
        table_chunk = re.sub(r'/\*.+?\*/', '', table_chunk)
        table_chunk = re.sub(r'\n\s*\-\-.*', '', table_chunk)
        table_chunk = re.sub(r'\n\s*\n', '\n', table_chunk)
        table_name = table_chunk.split('(')[0].strip()
        if not table_name or '\n' in table_name:
            continue
        if '(' not in table_chunk:
            continue
        indexes = {}
        for res in re.findall(
            r'create( +unique|)(?: +fulltext|) +index +(\S+?) +on +%s +\((.+?)\)\;' %
            table_name,
                table_chunk):
            indexes[res[1]] = {'unique': bool(res[0]), 'columns': res[2]}
        table_structure = re.split(
            r'create( +unique|) +index',
            '('.join(
                table_chunk.split('(')[
                    1:]))[0]
        table_structure_real = {}
        pk = None
        for line in table_structure.split('\n'):
            line = line.strip()
            if not line or line.endswith(';'):
                continue
            # Why strip(',') doesn't work?
            if line.endswith(','):
                line = line[:-1]
            if line.startswith('primary key'):
                pk = line.split('(')[1].split(')')[0].replace(' ', '')
                continue
            line = re.sub(r' +', ' ', line).split('--')[0]
            if line.split(' ')[1].startswith('enum'):
                real_type = ' '.join(line.split(')')[0].split(' ')[1:]) + ')'
                real_type = real_type.replace('"', '\'').replace(' ', '')
            else:
                real_type = line.split(' ')[1]
                if ' unsigned ' in line:
                    line = line.replace(' unsigned ', ' ')
                    real_type += ' unsigned'
            table_structure_real[line.split(' ')[0]] = {
                'type': real_type, 'config': ' '.join(line.split(' ')[2:])}

        result.append({'structure': table_structure_real,
                       'indexes': indexes, 'name': table_name})

    return result


def compare_table_with_prod(
        shard,
        host,
        table_name,
        expected_table_structure,
        table_sql,
        table_indexes,
        wiki):
    fields_in_prod = []
    return_result = {'fields': {}}
    for line in table_sql:
        if line.startswith('ERROR'):
            return return_result
        if not line or line.startswith('Field'):
            continue
        field_structure = line.lower().split('\t')
        fields_in_prod.append(field_structure[0])
        name = field_structure[0]
        if name not in expected_table_structure['structure']:
            add_to_drifts(shard, host, table_name, name,
                          'field-mismatch-prod-extra', wiki)
            continue
        return_result['fields'][field_structure[0]] = field_structure[1]
        if '--important-only' in sys.argv:
            continue
        expected_type = expected_table_structure['structure'][name]['type'].replace(
            'varchar', 'varbinary').replace('integer', 'int')
        if expected_type != field_structure[1].replace('varchar', 'varbinary'):
            actual_size = None
            if '(' in field_structure[1]:
                actual_size = field_structure[1].split('(')[1].split(')')[0]

            expected_size = None
            if '(' in expected_type:
                expected_size = expected_type.split('(')[1].split(')')[0]

            if actual_size and expected_size and actual_size != expected_size:
                add_to_drifts(
                    shard,
                    host,
                    table_name,
                    name,
                    'field-size-mismatch',
                    wiki)
            if (field_structure[1] + expected_type).count(' unsigned') == 1:
                add_to_drifts(shard, host, table_name, name,
                              'field-unsigned-mismatch', wiki)
            actual_type = field_structure[1].split('(')[0].split(' ')[0]
            expected_type = expected_type.split('(')[0].split(' ')[0]
            if actual_type != expected_type:
                add_to_drifts(
                    shard,
                    host,
                    table_name,
                    name,
                    'field-type-mismatch',
                    wiki)
        expected_config = expected_table_structure['structure'][name]['config']
        if (field_structure[2] == 'no' and 'not null' not in expected_config) or (
                field_structure[2] == 'yes' and 'not null' in expected_config):
            add_to_drifts(
                shard,
                host,
                table_name,
                name,
                'field-null-mismatch',
                wiki)

        # if len(field_structure[4]) < 4:
        #    default = ''
        # else:
        #    default = field_structure[4]
        # if default == 'null' and field_structure[2] == 'no':
        #    continue
        #print(default, expected_config)
        # if (default and 'default ' + default not in expected_config) or (not default and 'default ' in expected_config):
        #        print(host, table_name, name, 'field-default-mismatch')
        # print(expected_config)
    for field in expected_table_structure['structure']:
        if field not in fields_in_prod:
            add_to_drifts(shard, host, table_name, field,
                          'field-mismatch-codebase-extra', wiki)

    return_result['indexes'] = {}
    indexes = {}
    for line in table_indexes:
        if line.startswith('ERROR'):
            return return_result
        if not line or line.startswith('Table'):
            continue
        index_structure = line.lower().split('\t')

        if index_structure[2] not in indexes:
            indexes[index_structure[2]] = {
                'unique': index_structure[1] == '0', 'columns': [index_structure[4]]}
        else:
            indexes[index_structure[2]]['columns'].append(index_structure[4])
    return_result['indexes'] = indexes
    expected_indexes = expected_table_structure['indexes']
    for index in indexes:
        # clean up primaries later
        if index == 'primary':
            continue
        if index not in expected_indexes:
            if index == 'tmp1':
                print('wtf')
            add_to_drifts(shard, host, table_name, index,
                          'index-mismatch-prod-extra', wiki)
            continue
        if indexes[index]['unique'] != expected_indexes[index]['unique']:
            add_to_drifts(shard, host, table_name, index,
                          'index-uniqueness-mismatch', wiki)
        expected_columns = expected_indexes[index]['columns'].replace(' ', '')
        expected_columns = re.sub(r'\(.+?\)', '', expected_columns)
        if ','.join(indexes[index]['columns']) != expected_columns:
            add_to_drifts(shard, host, table_name, index,
                          'index-columns-mismatch', wiki)

    for index in expected_indexes:
        if index not in indexes:
            add_to_drifts(shard, host, table_name, index,
                          'index-mismatch-code-extra', wiki)

    return return_result


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
    return (result[0].split('\n'), result[1].split('\n'))


def compare_table_with_prod_abstract(
        shard,
        host,
        expected_table_structure,
        table_sql,
        table_indexes,
        wiki):
    fields_in_prod = []
    table_name = expected_table_structure['name']
    return_result = {'fields': {}}
    for line in table_sql:
        if line.startswith('ERROR'):
            return return_result
        if not line or line.startswith('Field'):
            continue
        field_structure = line.lower().split('\t')
        fields_in_prod.append(field_structure[0])
        name = field_structure[0]
        expected_column = None
        for column in expected_table_structure['columns']:
            if column['name'] == name:
                expected_column = column
                break
        else:
            add_to_drifts(shard, host, table_name, name,
                          'field-mismatch-prod-extra', wiki)
            continue
        return_result['fields'][field_structure[0]] = field_structure[1]
        if '--important-only' in sys.argv:
            continue

        expected_type = expected_column['type'].replace(
            'string', 'binary').replace('integer', 'int')
        expected_size = expected_column['options'].get('length', 0)

        if expected_type == 'binary' and not expected_column['options'].get(
                'fixed'):
            expected_type = 'varbinary'
        elif expected_type == 'blob':
            if expected_size < 256:
                expected_type = 'tinyblob'
            elif expected_size < 65536:
                expected_type = 'blob'
            elif expected_size < 16777216:
                expected_type = 'mediumblob'
        elif expected_type == 'mwtinyint':
            expected_type = 'tinyint'
        elif expected_type == 'mwenum':
            expected_type = 'enum'
            expected_size = set([
                i.lower() for i in expected_column['options'].get(
                    'CustomSchemaOptions', {}).get(
                    'enum_values', [])])
        elif expected_type == 'mwtimestamp':
            if expected_column['options'].get(
                    'CustomSchemaOptions', {}).get(
                    'allowInfinite', False):
                expected_type = 'varbinary'
            else:
                expected_type = 'binary'
            expected_size = 14
        elif expected_type in ('datetimetz'):
            expected_type = 'timestamp'
        expected_not_null = expected_column['options'].get('notnull', False)
        expected_unsigned = expected_column['options'].get('unsigned', False)
        expected_auto_increment = expected_column['options'].get(
            'autoincrement', False)
        if expected_type != field_structure[1].replace('varchar', 'varbinary'):
            actual_size = None
            if '(' in field_structure[1]:
                actual_size = field_structure[1].split('(')[1].split(')')[0]
                if ',' in actual_size:
                    actual_size = set(actual_size.replace(
                        '\'', '').replace('"', '').split(','))

            if actual_size and expected_size:
                if not isinstance(expected_size, set):
                    the_same = int(actual_size) == int(expected_size)
                else:
                    the_same = actual_size == expected_size
                if not the_same:
                    add_to_drifts(shard, host, table_name, name,
                                  'field-size-mismatch', wiki)
            if (expected_unsigned and ' unsigned' not in field_structure[1]) or (
                    expected_unsigned is False and ' unsigned' in field_structure[1]):
                add_to_drifts(shard, host, table_name, name,
                              'field-unsigned-mismatch', wiki)
            actual_type = field_structure[1].split('(')[0].split(' ')[0]
            if actual_type != expected_type:
                add_to_drifts(
                    shard,
                    host,
                    table_name,
                    name,
                    'field-type-mismatch',
                    wiki)
                print(actual_type, expected_type)
        if (field_structure[2] == 'no' and expected_not_null is not True) or (
                field_structure[2] == 'yes' and expected_not_null is not False):
            add_to_drifts(
                shard,
                host,
                table_name,
                name,
                'field-nullable-mismatch',
                wiki)

    for column in expected_table_structure['columns']:
        if column['name'] not in fields_in_prod:
            add_to_drifts(
                shard,
                host,
                table_name,
                column['name'],
                'field-mismatch-codebase-extra',
                wiki)

    return_result['indexes'] = {}
    indexes = {}
    for line in table_indexes:
        if line.startswith('ERROR'):
            return return_result
        if not line or line.startswith('Table'):
            continue
        index_structure = line.lower().split('\t')

        if index_structure[2] not in indexes:
            indexes[index_structure[2]] = {
                'unique': index_structure[1] == '0', 'columns': [index_structure[4]]}
        else:
            indexes[index_structure[2]]['columns'].append(index_structure[4])
    return_result['indexes'] = indexes
    expected_indexes = expected_table_structure['indexes']
    expected_pk = expected_table_structure.get('pk')
    for index in indexes:
        # clean up primaries later
        if index == 'primary':
            if indexes[index]['columns'] != expected_pk:
                add_to_drifts(shard, host, table_name, index,
                              'primary-key-mismatch', wiki)
            continue
        expected_index = None
        for i in expected_indexes:
            if i['name'] == index:
                expected_index = i
                break
        else:
            add_to_drifts(shard, host, table_name, index,
                          'index-mismatch-prod-extra', wiki)
            continue
        if indexes[index]['unique'] != expected_index['unique']:
            add_to_drifts(shard, host, table_name, index,
                          'index-uniqueness-mismatch', wiki)
        expected_columns = expected_index['columns']
        if indexes[index]['columns'] != expected_columns:
            add_to_drifts(shard, host, table_name, index,
                          'index-columns-mismatch', wiki)

    for index in expected_indexes:
        if index['name'] not in indexes:
            add_to_drifts(shard, host, table_name,
                          index['name'], 'index-mismatch-code-extra', wiki)

    return return_result


def dispatching_compare_table_with_prod(shard, host, table, sql_command, wiki):
    table_structure = get_table_structure_sql(host, sql_command, table['name'])
    table_sql = table_structure[0]
    table_indexes = table_structure[1]
    if not table_sql or not table_indexes:
        print('no response')
        return {}
    if '--abstract' in sys.argv:
        return compare_table_with_prod_abstract(
            shard, host, table, table_sql, table_indexes, wiki)
    return compare_table_with_prod(
        shard, host, table['name'], table, table_sql, table_indexes, wiki)


def handle_wiki(shard, sql_data, hosts, wiki, final_result, sql_command):
    if shard is not None:
        sql_command = sql_command.format(wiki=wiki)
        debug('Sql command for this shard', sql_command)
    for host in hosts:
        final_result[host] = {}
        for table in sql_data:
            if table['name'] == 'searchindex':
                continue
            print(host, table['name'])
            final_result[host][table['name']] = dispatching_compare_table_with_prod(
                shard, host, table, sql_command, wiki)
            time.sleep(1)

    for host in hosts:
        for table in final_result[hosts[0]]:
            if final_result[host][table] != final_result[hosts[0]][table]:
                dd(final_result[host][table],
                   final_result[hosts[0]][table], table + ':' + host)
    return final_result


def handle_shard(shard, sql_data, hosts, all_=False):
    final_result = {}
    sql_command = sys.argv[2]
    if shard is not None:
        wikis = get_a_wiki_from_shard(shard, all_)
    else:
        wikis = ['']
    for wiki in wikis:
        final_result = handle_wiki(
            shard,
            sql_data,
            hosts,
            wiki,
            final_result,
            sql_command)


def main():
    with open('by_db_drifts.json', 'w') as f:
        f.write(json.dumps({}))
    with open('by_drift_type_drifts.json', 'w') as f:
        f.write(json.dumps({}))
    shard_mapping = get_shard_mapping()
    if '--abstract' not in sys.argv:
        if sys.argv[1].lower() in type_to_path_mapping:
            sql = get_sql_from_gerrit(
                type_to_path_mapping[sys.argv[1].lower()])
        else:
            with open(sys.argv[1], 'r') as f:
                sql = f.read()
        sql_data = parse_sql(sql)
    else:
        if sys.argv[1].lower() in type_to_path_mapping_abstracts:
            sql_data = json.loads(get_sql_from_gerrit(
                type_to_path_mapping_abstracts[sys.argv[1].lower()]))
        else:
            with open(sys.argv[1], 'r') as f:
                sql_data = json.loads(f.read())
    if '-prod' in sys.argv:
        if sys.argv[3] == 'all':
            for shard in shard_mapping:
                handle_shard(shard, sql_data, shard_mapping[shard], False)
        else:
            hosts = shard_mapping[sys.argv[3]]
            handle_shard(sys.argv[3], sql_data, hosts, True)
    else:
        handle_shard(None, sql_data, ['localhost'])


main()
