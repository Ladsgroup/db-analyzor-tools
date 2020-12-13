import json

import pywikibot

with open('/var/lib/mediawiki/maintenance/tables.json', 'r') as f:
    tables = json.loads(f.read())
tables = sorted(tables, key=lambda t: t['name'])
text = '__NOTOC__'
for table in tables:
    text += '=={}==\n'.format(table['name'])
    if table.get('comment'):
        text += table['comment'] + '\n'
    if table.get('pk'):
        text += "\n'''Primary Key:''' <code>" + \
            ', '.join(table['pk']) + '</code>\n'
    text += '\n===Columns===\n{| class="wikitable"\n!Name!!Type!!Nullable!!Default!!Extra options!!Description\n|-\n'
    for column in table['columns']:
        extra_options = []
        if column['options'].get('length'):
            extra_options.append('Length: ' + str(column['options']['length']))
        if column['options'].get('unsigned'):
            if column['options']['unsigned']:
                extra_options.append('Unsigned')
            else:
                extra_options.append('Signed')
        if 'default' in column['options']:
            if column['options']['default'] == '':
                default = '""'
            elif column['options']['default'] is None:
                default = 'NULL'
            else:
                default = column['options']['default']
        else:
            default = 'No default'
        if column['options'].get('notnull', False) == False:
            nullable = 'Yes'
        else:
            nullable = 'No'
        text += '|{}||{}||{}||{}||{}||{}'.format(
            '<code>{}</code>'.format(column['name']),
            column['type'],
            nullable,
            default,
            '\n'.join(extra_options),
            column.get('comment', ''),
        ) + '\n'
        text += '|-\n'
    text += '|}\n'

    if not table.get('indexes'):
        continue
    text += '\n===Indexes===\n{| class="wikitable"\n!Name!!Columns!!Unique!!Description\n|-\n'
    for index in table['indexes']:
        if index.get('unique', False) == False:
            unique = 'No'
        else:
            unique = 'Yes'
        text += '|{}||{}||{}||{}'.format(
            '<code>{}</code>'.format(column['name']),
            '<code>{}</code>'.format(', '.join(index['columns'])),
            unique,
            index.get('comment', ''),
        ) + '\n'
        text += '|-\n'
    text += '|}\n'

site = pywikibot.Site('mediawiki', 'mediawiki')
page = pywikibot.Page(site, 'User:Ladsgroup/Test')
page.put(text)
page.save()
