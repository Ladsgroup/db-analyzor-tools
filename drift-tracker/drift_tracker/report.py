import time

from .sources import get_drifts, get_tracking

# Drift types written by db_drift_checker.py, {0} is the table, {1} the field or index
TITLES = {
    'field-type-mismatch': 'Mismatching field type of {0}.{1}',
    'field-nullable-mismatch': 'Mismatching field nullability of {0}.{1}',
    'field-unsigned-mismatch': 'Mismatching field unsigned status of {0}.{1}',
    'field-size-mismatch': 'Mismatching field size of {0}.{1}',
    'field-auto-increment-mismatch': 'Mismatching auto increment of {0}.{1}',
    'field-mismatch-codebase-extra': 'Extra field {0}.{1} in code',
    'field-mismatch-prod-extra': 'Extra field {0}.{1} in production',
    'primary-key-mismatch': 'Mismatching primary key of {0}',
    'index-mismatch-prod-extra': 'Extra index {1} in production on table {0}',
    'index-mismatch-code-extra': 'Extra index {1} in codebase on table {0}',
    'index-uniqueness-mismatch': 'Mismatching uniqueness of index {1} on table {0}',
    'index-columns-mismatch': 'Mismatching columns of index {1} on table {0}',
}

# Drifts in more sections than this are "widespread"
WIDESPREAD = 5

# Wikis listed per checklist item, the rest is only counted
CHECKLIST_WIKIS = 5


def parse_drift(code, drift, tracking):
    # Code is "<table> <field or index> <type>"; the middle part can be empty
    parts = code.split(' ')
    table, type_ = parts[0], parts[-1]
    piece = ' '.join(parts[1:-1]).strip()
    title = TITLES.get(type_)
    hosts = []
    for section in sorted(drift):
        for host_wiki in drift[section]:
            host, _, wiki = host_wiki.rpartition(':')
            hosts.append({'section': section, 'host': host, 'wiki': wiki})
    return {
        'code': code,
        'name': title.format(table, piece) if title else code,
        'table': table,
        'type': type_,
        'sections': sorted(drift),
        'hosts': hosts,
        'tracked': tracking.get(code),
        'widespread': len(drift) > WIDESPREAD,
    }


def compact_hosts(hosts):
    """Rows of (section, hosts, wikis); hosts of a section with the same wikis share a row."""
    by_section = {}
    for h in hosts:
        by_section.setdefault(h['section'], {}).setdefault(h['host'], set()).add(h['wiki'])
    rows = []
    for section in sorted(by_section):
        by_wikis = {}
        for host, wikis in by_section[section].items():
            by_wikis.setdefault(frozenset(wikis), []).append(host)
        for wikis, hosts_ in sorted(by_wikis.items(), key=lambda i: sorted(i[1])):
            rows.append({'section': section, 'hosts': sorted(hosts_), 'wikis': sorted(wikis)})
    return rows


def group_drifts(drifts):
    """Group drifts that affect exactly the same hosts, keeping drift order."""
    groups = {}
    for d in drifts:
        key = frozenset((h['section'], h['host'], h['wiki']) for h in d['hosts'])
        groups.setdefault(key, []).append(d)
    result = []
    for members in groups.values():
        hosts = members[0]['hosts']
        result.append({
            'drifts': members,
            'rows': compact_hosts(hosts),
            'sections': members[0]['sections'],
            'host_count': len({h['host'] for h in hosts}),
            'wiki_count': len({h['wiki'] for h in hosts}),
        })
    return result


def phabricator_checklist(category, groups, url, end_time):
    """Remarkup summary of all drifts, one checklist item per host row."""
    lines = ['Schema drifts of `{}` found by {}{}.'.format(
        category, url, ' (report of {})'.format(end_time) if end_time else '')]
    for group in groups:
        lines.append('')
        for d in group['drifts']:
            tracked = ' (tracked in {})'.format(d['tracked']) if d['tracked'] else ''
            lines.append('* **{}** `{}`{}'.format(d['name'], d['code'], tracked))
        lines.append('')
        for row in group['rows']:
            wikis = ', '.join(row['wikis'][:CHECKLIST_WIKIS])
            if len(row['wikis']) > CHECKLIST_WIKIS:
                wikis += ' and {} more'.format(len(row['wikis']) - CHECKLIST_WIKIS)
            lines.append('[ ] {}: {} ({})'.format(row['section'], ', '.join(row['hosts']), wikis))
    return '\n'.join(lines) + '\n'


def format_time(timestamp):
    if not timestamp:
        return None
    return time.strftime('%d %b %Y %H:%M UTC', time.gmtime(timestamp))


def get_report(category):
    """Parsed drift report of a category, or None if it is not published."""
    data = get_drifts(category)
    if data is None:
        return None
    tracking = get_tracking()
    drifts = [
        parse_drift(code, drift, tracking)
        for code, drift in data.items() if not code.startswith('_')
    ]
    drifts.sort(key=lambda d: (-len(d['sections']), -len(d['hosts']), d['code']))

    metadata = data.get('_metadata', {})
    start, end = metadata.get('time_start'), metadata.get('time_end')
    return {
        'drifts': drifts,
        'groups': group_drifts(drifts),
        'types': sorted({d['type'] for d in drifts}),
        'stats': {
            'total': len(drifts),
            'untracked': sum(1 for d in drifts if not d['tracked']),
            'widespread': sum(1 for d in drifts if d['widespread']),
            'untracked_widespread': sum(
                1 for d in drifts if d['widespread'] and not d['tracked']),
        },
        'start_time': format_time(start),
        'end_time': format_time(end),
        'duration': '{:.2f}'.format((end - start) / 3600) if start and end else None,
    }
