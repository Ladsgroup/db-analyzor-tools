#!/usr/bin/python3

import base64
import json
import random
import re
import subprocess
import sys
import time
from collections import defaultdict
from optparse import OptionParser

import requests


def parse_sql(sql):
    result = {}
    sql = sql.replace('IF NOT EXISTS ', '')
    sql = re.sub(r'\s*\-\-.*', '', sql)

    for table_chunk in sql.split('CREATE TABLE '):
        table_chunk = table_chunk.lower()
        table_chunk = re.sub(r'/\*.+?\*/', '', table_chunk)
        table_chunk = re.sub(r'\n\s*\n', '\n', table_chunk)

        table_name = table_chunk.split('(')[0].strip()

        if not table_name or '\n' in table_name:
            continue
        if '(' not in table_chunk:
            continue
        indexes = {}

        for res in re.findall(r'create( +unique|)(?: +fulltext|) +index +(\S+?)[ \n]+on +%s +\((.+?)\)\;' % table_name, table_chunk):
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

            lineSplitSpace = line.split(' ')

            opts = {}
            if 'primary key' in line:
                if line.startswith('primary key'):
                    pk = line.split('(')[1].split(')')[0].replace(' ', '')
                    continue
                else:
                    pk = lineSplitSpace[0]
                    if 'auto_increment' in line:
                        opts['autoincrement'] = True

            line = re.sub(r' +', ' ', line)

            if lineSplitSpace[1].startswith('enum'):
                real_type = ' '.join(line.split(')')[0].split(' ')[1:]) + ')'
                real_type = real_type.replace('"', '\'').replace(' ', '')
            else:
                real_type = lineSplitSpace[1]

            if ' unsigned ' in line:
               line = line.replace(' unsigned ', ' ')
               opts['unsigned'] = True

            if 'not null' in line:
               line = line.replace('not null', ' ')
               opts['notnull'] = True

            table_structure_real[lineSplitSpace[0]] = {
                'type': real_type,
                'config': ' '.join(lineSplitSpace[2:]),
                'options': opts,
            }

        result[table_name] = {
           'structure': table_structure_real,
           'indexes': indexes
        }
        if pk is not None:
            result[table_name]['pk'] = pk

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
        return {'type': 'integer', 'length': None}
    if type_ == 'varbinary':
        return {'type': 'binary', 'length': None}
    if type_ == 'bool':
        return {'type': 'boolean', 'length': None}
    if type_ == 'timestamp':
        return {'type': 'time', 'length': None}
    if type_ == 'tinyint':
        return {'type': 'smallint', 'length': None}
    if type_ == 'varchar':
        return {'type': 'string', 'length': None}
    if type_ == 'enum':
        return {'type': 'smallint', 'length': None}
    if type_ == 'tinyblob':
        return {'type': 'blob', 'length': 255}
    return {'type': type_, 'length': None}


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("--sqlfile", help="sql file to process", metavar="FILE")

    opts, args = parser.parse_args()

    # If --sqlfile is passed, parse that, else parse MW core from gerrit (old behaviour)
    if opts.sqlfile:
        sqlfile = open(opts.sqlfile,'r')
        sql = sqlfile.read()
    else:
        sql = get_sql_from_gerrit('core')

    parsed = parse_sql(sql)

    final_result = []

    for table in parsed:
        table_abstract = {}
        table_abstract['name'] = table

        columns = []
        for column_name in parsed[table]['structure']:
            column = {'name': column_name}
            type = get_type(parsed[table]['structure'][column_name]['type'])
            column['type'] = type['type']
            column['options'] = parsed[table]['structure'][column_name]['options']
            if type['length'] is not None:
                column['options']['length'] = type['length']
            columns.append(column)

        table_abstract['columns'] = columns

        indexes = []
        for index_name in parsed[table]['indexes']:
            index = {'name': index_name}
            index['columns'] = parsed[table]['indexes'][index_name]['columns'].split(',')
            if (parsed[table]['indexes'][index_name]['unique']):
                index['unique'] = True
            indexes.append(index)

        table_abstract['indexes'] = indexes

        if parsed[table]['pk']:
            table_abstract['pk'] = [ parsed[table]['pk'] ]

        final_result.append(table_abstract)

    print(json.dumps(final_result, indent='\t'))
