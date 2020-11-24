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

            line = re.sub(r' +', ' ', line)
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

            # KEY() in CREATE TABLE() doesn't have an index name...
            # Should probably autogenerate a name, so it can be fixed manually in the json
            # For now, just skip that line
            elif re.search('key +\(', line):
                 continue
            #elif 'key(' in line:
            #    indexes[''] = line.split('(')[1].split(')')[0].replace(' ', '')

            if lineSplitSpace[1].startswith('enum'):
                real_type = ' '.join(line.split(')')[0].split(' ')[1:]) + ')'
                real_type = real_type.replace('"', '\'').replace(' ', '')
            else:
                real_type = lineSplitSpace[1]

            if ' unsigned ' in line:
                line = line.replace(' unsigned ', ' ')
                opts['unsigned'] = True

            not_null = 'not null' in line
            if not_null:
                line = line.replace('not null', ' ')
            opts['notnull'] = not_null

            if ' default' in line:
               default = re.findall('default +(.+?)(?:\s|$)', line)[0]
               if '\'' in default:
                   default = str(default).replace('\'', '')

               if default.isnumeric():
                   default = int(default)
               elif default == 'null':
                   default = None

               opts['default'] = default

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

def get_type(type_, name):
    split = type_.split(' ')[0]
    length = None
    if '(' in split:
        bracketsval = split.split('(')[1].split(')')[0]
        if bracketsval.isnumeric():
            length = int(bracketsval)

    type_ = split.split('(')[0]

    if 'timestamp' in name:
        return {'type':'mwtimestamp', 'length':14}

    if type_ == 'int':
        return {'type': 'integer', 'length': length}
    if type_ == 'varbinary':
        return {'type': 'binary', 'length': length}
    if type_ == 'bool':
        return {'type': 'boolean', 'length': length}
    if type_ == 'timestamp':
        return {'type': 'time', 'length': length}
    if type_ == 'tinyint':
        # mwtinyint is a custom MW datatype
        return {'type': 'mwtinyint', 'length': length}
    if type_ == 'varchar':
        return {'type': 'string', 'length': length}
    if type_ == 'enum':
        return {'type': 'smallint', 'length': None}
    if type_ == 'tinyblob':
        return {'type': 'blob', 'length': 255}
    if type_ == 'mediumblob':
        return {'type': 'blob', 'length': 16777215}
    if type_ == 'char':
        return {'type': 'varchar', 'length': length}
    return {'type': type_, 'length': length}


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
        table_abstract = {
            'name': table,
            #'comment': '',
        }

        columns = []
        for column_name in parsed[table]['structure']:
            type = get_type(parsed[table]['structure'][column_name]['type'], column_name)
            column = {
                 'name': column_name,
                 #'comment': '',
                'type': type['type'],
                'options': parsed[table]['structure'][column_name]['options'],
            }
            if type['length'] is not None:
                column['options']['length'] = type['length']
            columns.append(column)

        table_abstract['columns'] = columns

        indexes = []
        for index_name in parsed[table]['indexes']:
            index = {
                'name': index_name,
                #'comment': '',
                'columns': parsed[table]['indexes'][index_name]['columns'].replace(' ', '').split(','),
                'unique': parsed[table]['indexes'][index_name]['unique'],
            }
            indexes.append(index)

        table_abstract['indexes'] = indexes

        if parsed[table]['pk']:
            table_abstract['pk'] = [ parsed[table]['pk'] ]

        final_result.append(table_abstract)

    print(json.dumps(final_result, indent='\t'))
