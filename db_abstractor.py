import base64
import json
import random
import re
import subprocess
import sys
import time
from collections import defaultdict

import requests


def parse_sql(sql):
    result = {}
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

            result[table_name] = {
                'structure': table_structure_real, 'indexes': indexes}

    return result

gerrit_url = 'https://gerrit.wikimedia.org/g/'
type_to_path_mapping = {
    'core': 'mediawiki/core/+/master/maintenance/tables.sql',
    'wikibase-repo': 'mediawiki/extensions/Wikibase/+/master/repo/sql/Wikibase.sql',
    'wikibase-client': 'mediawiki/extensions/Wikibase/+/master/client/sql/entity_usage.sql',
    'abusefilter': 'mediawiki/extensions/AbuseFilter/+/master/abusefilter.tables.sqlite.sql',
    'flaggedrevs': 'mediawiki/extensions/FlaggedRevs/+/master/backend/schema/mysql/FlaggedRevs.sql',
}


def get_sql_from_gerrit(type_):
    url = gerrit_url + '{0}?format=TEXT'.format(type_to_path_mapping[type_])
    return base64.b64decode(requests.get(url).text).decode('utf-8')

def get_type(type_):
    type_ = type_.split(' ')[0].split('(')[0]
    if type_ == 'int':
        return 'integer'
    if type_ == 'varbinary':
        return 'binary'
    if type_ == 'bool':
        return 'boolean'
    if type_ == 'timestamp':
        return 'time'
    if type_ == 'tinyint':
        return 'smallint'
    if type_ == 'varchar':
        return 'string'
    if type_ == 'enum':
        return 'smallint'
    return type_

sql = get_sql_from_gerrit('core')
parsed = parse_sql(sql)
final_result = []
for table in parsed:
    table_abstract = {}
    table_abstract['name'] = table
    columns = []
    for column_name in parsed[table]['structure']:
        column = {'name': column_name}
        column['type'] = get_type(parsed[table]['structure'][column_name]['type'])
        columns.append(column)
    table_abstract['columns'] = columns
    final_result.append(table_abstract)

print(json.dumps(final_result, indent='\t'))
