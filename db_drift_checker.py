import sys
import re
import time
from subprocess import run, PIPE, TimeoutExpired
import json
import base64
from collections import defaultdict
import random

import requests

# Something like this: python3 new_db_checker.py core "sql {wiki} -- " s8 --important-only -prod
# for localhost: python3 new_db_checker.py core "sudo mysql -Drepo " s8 --important-only -v


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
}
by_db_drifts = {}
by_drift_type_drifts = defaultdict(dict)


def get_a_wiki_from_shard(shard):
    debug('Getting a wiki from shard:', shard)
    url = gerrit_url + \
        'operations/mediawiki-config/+/master/dblists/{shard}.dblist?format=TEXT'.format(
            shard=shard)
    dbs = base64.b64decode(requests.get(url).text).decode('utf-8').split('\n')
    random.shuffle(dbs)
    for line in dbs:
        if not line or line.startswith('#'):
            continue
        debug('Got this wiki:', line.strip())
        return line.strip()


def add_to_drifts(shard, db, table, second, drift_type):
    drift = ' '.join([table, second, drift_type])
    if shard not in by_db_drifts:
        by_db_drifts[shard] = defaultdict(list)
    by_db_drifts[shard][db].append(drift)

    if shard not in by_drift_type_drifts[drift]:
        by_drift_type_drifts[drift][shard] = []
    by_drift_type_drifts[drift][shard].append(db)

    with open('by_db_drifts.json', 'w') as f:
        f.write(json.dumps(by_db_drifts, indent=4, sort_keys=True))
    with open('by_drift_type_drifts.json', 'w') as f:
        f.write(json.dumps(by_drift_type_drifts, indent=4, sort_keys=True))


def get_sql_from_gerrit(url):
    url = gerrit_url + '{0}?format=TEXT'.format(url)
    return base64.b64decode(requests.get(url).text).decode('utf-8')


def get_shard_mapping():
    shard_mapping = {}
    db_eqiad_data = requests.get(
        'https://noc.wikimedia.org/dbconfig/eqiad.json').json()
    for shard in db_eqiad_data['sectionLoads']:
        cases = []
        if shard == 'DEFAULT':
            name = 's3'
        else:
            name = shard
        for type_ in db_eqiad_data['sectionLoads'][shard]:
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
                if type(d1[k]) != type(d2[k]):
                    print(k + " changed in " + ctx + " to " + str(d2[k]))
                    continue
                else:
                    if type(d2[k]) == dict:
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
        for res in re.findall(r'create( +unique|)(?: +fulltext|) +index +(\S+?) +on +%s +\((.+?)\)\;' % table_name, table_chunk):
            indexes[res[1]] = {'unique': bool(res[0]), 'columns': res[2]}
        table_structure = re.split(
            r'create( +unique|) +index', '('.join(table_chunk.split('(')[1:]))[0]
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

        result.append({
            'structure': table_structure_real, 'indexes': indexes, 'name': table_name})

    return result


def compare_table_with_prod(shard, host, table_name, expected_table_structure, table_sql, table_indexes):
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
                          'field-mismatch-prod-extra')
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
                add_to_drifts(shard, host, table_name, name, 'field-size-mismatch',
                              expected_size + ' ' + actual_size)
            if (field_structure[1] + expected_type).count(' unsigned') == 1:
                add_to_drifts(shard, host, table_name, name, 'field-unsigned-mismatch',
                              field_structure[1] + ' ' + expected_type)
            actual_type = field_structure[1].split('(')[0].split(' ')[0]
            expected_type = expected_type.split('(')[0].split(' ')[0]
            if actual_type != expected_type:
                add_to_drifts(shard, host, table_name, name, 'field-type-mismatch',
                              expected_type + ' ' + actual_type)
        expected_config = expected_table_structure['structure'][name]['config']
        if (field_structure[2] == 'no' and 'not null' not in expected_config) or (field_structure[2] == 'yes' and 'not null' in expected_config):
            add_to_drifts(shard, host, table_name, name, 'field-null-mismatch')

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
                          'field-mismatch-codebase-extra')

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
                          'index-mismatch-prod-extra')
            continue
        if indexes[index]['unique'] != expected_indexes[index]['unique']:
            add_to_drifts(shard, host, table_name, index,
                          'index-uniqueness-mismatch')
        expected_columns = expected_indexes[index]['columns'].replace(' ', '')
        expected_columns = re.sub(r'\(.+?\)', '', expected_columns)
        if ','.join(indexes[index]['columns']) != expected_columns:
            add_to_drifts(shard, host, table_name, index,
                          'index-columns-mismatch')

    for index in expected_indexes:
        if index not in indexes:
            add_to_drifts(shard, host, table_name, index,
                          'index-mismatch-code-extra')

    return return_result


def get_table_structure_sql(host, sql_command, table_name):
    port = None
    if host != 'localhost':
        if re.search(r' \-\-(?: |$)', sql_command):
            sql_command = re.split(r' \-\-(?: |$)', sql_command)[
                0] + ' --host ' + host + ' -- ' + re.split(r' \-\-(?: |$)', sql_command)[1]
        if ':' in host:
            port = host.split(':')[1]
            host = host.split(':')[0]
        host += '.eqiad.wmnet'
    debug('Checking table ', table_name)
    if port:
        sql_command += ' -P ' + port
    command = 'timeout 6 ' + sql_command + ' -h %s -e "DESC %s;"' % (host, table_name)
    debug('Running:', command)
    try:
        res = run(command, stdin=PIPE, stdout=PIPE, shell=True, stderr=PIPE, timeout=5)
    except TimeoutExpired:
        debug('First timeout has reached')
        try:
            res = run(command, stdin=PIPE, stdout=PIPE, shell=True, stderr=PIPE, timeout=5)
        except:
            return {}
    if res.stderr and res.stderr.decode('utf-8'):
        return {}
    return res.stdout.decode('utf-8').split('\n')


def get_table_indexes_sql(host, sql_command, table_name):
    command = 'timeout 6 ' + sql_command + ' -h %s -e "SHOW INDEX FROM %s;"' % (host, table_name)
    debug('Running:', command)
    try:
        res = run(command, stdin=PIPE, stdout=PIPE, shell=True, stderr=PIPE, timeout=5)
    except TimeoutExpired:
        debug('First timeout has reached')
        try:
            res = run(command, stdin=PIPE, stdout=PIPE, shell=True, stderr=PIPE, timeout=5)
        except TimeoutExpired:
            return {}
    if res.stderr and res.stderr.decode('utf-8'):
        return {}
    return res.stdout.decode('utf-8').split('\n')


def compare_table_with_prod_abstract(shard, host, expected_table_structure, table_sql, table_indexes):
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
        expected_column = None
        for column in expected_table_structure['columns']:
            if column['name'] == name:
                expected_column = column
                break
        else:
            add_to_drifts(shard, host, table_name, name,
                          'field-mismatch-prod-extra')
            continue
        return_result['fields'][field_structure[0]] = field_structure[1]
        if '--important-only' in sys.argv:
            continue

        # TODO: Clean up
        expected_type = expected_column['type'].replace(
            'string', 'varbinary').replace('integer', 'int')
        if expected_type != field_structure[1].replace('varchar', 'varbinary'):
            actual_size = None
            if '(' in field_structure[1]:
                actual_size = field_structure[1].split('(')[1].split(')')[0]

            expected_size = None
            if '(' in expected_type:
                expected_size = expected_type.split('(')[1].split(')')[0]

            if actual_size and expected_size and actual_size != expected_size:
                add_to_drifts(shard, host, table_name, name, 'field-size-mismatch',
                              expected_size + ' ' + actual_size)
            if (field_structure[1] + expected_type).count(' unsigned') == 1:
                add_to_drifts(shard, host, table_name, name, 'field-unsigned-mismatch',
                              field_structure[1] + ' ' + expected_type)
            actual_type = field_structure[1].split('(')[0].split(' ')[0]
            expected_type = expected_type.split('(')[0].split(' ')[0]
            if actual_type != expected_type:
                add_to_drifts(shard, host, table_name, name, 'field-type-mismatch',
                              expected_type + ' ' + actual_type)
        expected_config = expected_table_structure['structure'][name]['config']
        if (field_structure[2] == 'no' and 'not null' not in expected_config) or (field_structure[2] == 'yes' and 'not null' in expected_config):
            add_to_drifts(shard, host, table_name, name, 'field-null-mismatch')
        # Until here

    for column in expected_table_structure['columns']:
        if column['name'] not in fields_in_prod:
            add_to_drifts(shard, host, table_name,
                          column['name'], 'field-mismatch-codebase-extra')

    return_result['indexes'] = {}
    indexes = {}
    for line in table_iexpected_indexesexes:
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
        expected_index = None
        for i in expected_indexes:
            if i['name'] == index:
                expected_index = i
                break
        else:
            add_to_drifts(shard, host, table_name, index,
                          'index-mismatch-prod-extra')
            continue
        if indexes[index]['unique'] != expected_index['unique']:
            add_to_drifts(shard, host, table_name, index,
                          'index-uniqueness-mismatch')
        expected_columns = expected_index['columns']
        if indexes[index]['columns'] != expected_columns:
            add_to_drifts(shard, host, table_name, index,
                          'index-columns-mismatch')

    for index in expected_indexes:
        if index['name'] not in indexes:
            add_to_drifts(shard, host, table_name,
                          index['name'], 'index-mismatch-code-extra')

    return return_result


def dispatching_compare_table_with_prod(shard, host, table, sql_command):
    table_sql = get_table_structure_sql(host, sql_command, table['name'])
    table_indexes = get_table_indexes_sql(host, sql_command, table['name'])
    if not table_sql or not table_indexes:
        print('no response')
        return {}
    if '--abstract' in sys.argv:
        return compare_table_with_prod_abstract(
            shard, host, table, table_sql, table_indexes)
    return compare_table_with_prod(
        shard, host, table['name'], table, table_sql, table_indexes)


def handle_shard(shard, sql_data, hosts):
    final_result = {}
    sql_command = sys.argv[2]
    if shard != None:
        sql_command = sql_command.format(wiki=get_a_wiki_from_shard(shard))
        debug('Sql command for this shard', sql_command)
    for host in hosts:
        final_result[host] = {}
        for table in sql_data:
            if table['name'] == 'searchindex':
                continue
            print(host, table['name'])
            final_result[host][table['name']] = dispatching_compare_table_with_prod(
                shard, host, table, sql_command)
            time.sleep(1)

    for host in hosts:
        for table in final_result[hosts[0]]:
            if final_result[host][table] != final_result[hosts[0]][table]:
                dd(final_result[host][table],
                   final_result[hosts[0]][table], table + ':' + host)


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
                handle_shard(shard, sql_data, shard_mapping[shard])
        else:
            hosts = shard_mapping[sys.argv[3]]
            handle_shard(sys.argv[3], sql_data, hosts)
    else:
        handle_shard(None, sql_data, ['localhost'])


main()
