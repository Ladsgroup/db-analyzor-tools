import re
import sys
from subprocess import PIPE, TimeoutExpired, run


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
    if port:
        sql_command += ' -P ' + port
    command = 'timeout 6 ' + sql_command + \
        ' -h %s -e "DESC %s; SHOW INDEX FROM %s;"' % (
            host, table_name, table_name)
    try:
        res = run(
            command,
            stdin=PIPE,
            stdout=PIPE,
            shell=True,
            stderr=PIPE,
            timeout=5)
    except TimeoutExpired:
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
