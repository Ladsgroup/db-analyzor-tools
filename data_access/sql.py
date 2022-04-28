import re
import sys
from subprocess import PIPE, TimeoutExpired, run


def get_table_structure_sql(host, sql_command, db, dc):
    port = None
    if host != 'localhost':
        if re.search(r' \-\-(?: |$)', sql_command):
            sql_command = re.split(r' \-\-(?: |$)', sql_command)[
                0] + ' -- ' + re.split(r' \-\-(?: |$)', sql_command)[1]
        if ':' in host:
            port = host.split(':')[1]
            host = host.split(':')[0]
        host += '.{}.wmnet'.format(dc)
    if port:
        sql_command += ' -P ' + port
    command = 'timeout 6 {} -h {} -e ' + \
        '"select * FROM information_schema.columns WHERE table_schema = \'{}\'\\G; ' + \
        'SELECT * FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = \'{}\'\\G;"'
    command = command.format(sql_command, host, db, db)
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
            return ''
    if res.stderr and res.stderr.decode('utf-8'):
        return ''
    return res.stdout.decode('utf-8')
