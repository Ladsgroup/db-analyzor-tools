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
