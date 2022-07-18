from collections import OrderedDict

import requests

from .tracking import get_tracking_internal

titles = {
    'field-type-mismatch': 'Mismatching field type of {0}.{1}',
    'field-nullable-mismatch': 'Mismatching field nullability of {0}.{1}',
    'field-unsigned-mismatch': 'Mismatching field unsigned status of {0}.{1}',
    'field-mismatch-codebase-extra': 'Extra field {0}.{1} in code',
    'field-mismatch-prod-extra': 'Extra field {0}.{1} in production',
    'field-size-mismatch': 'Mismatching field size of {0}.{1}',
    'index-mismatch-prod-extra': 'Extra index {1} in production on table {0}',
    'index-mismatch-code-extra': 'Extra index {1} in codebase on table {0}',
}


def get_report(category, untracked_only=False):
    data = requests.get(
        'https://people.wikimedia.org/~ladsgroup/drifts_{category}.json'.format(category)).json()
    tracked = get_tracking_internal()
    metadata = data.get('_metadata', {})
    data = OrderedDict(sorted(
        [i for i in data.items() if i[0][0] != '_'],
        key=lambda t: len(t[1]),
        reverse=True))
    report = []
    for drift_name in data:
        if drift_name in tracked and untracked_only:
            continue
        drift = data[drift_name]
        drift_parts = drift_name.replace('  ', ' ').split(' ')
        drift_report = {
            'name': titles.get(drift_parts[-1], drift_name).format(drift_parts[0], drift_parts[1]),
            'section_count': len(drift),
            'sections': ', '.join(drift.keys()),
            'tracked': tracked.get(drift_name, False),
            'table': [],
            'code': drift_name
        }
        for section in drift:
            for host_report in drift[section]:
                drift_report['table'].append(
                    (
                        section,
                        ':'.join(host_report.split(':')[:-1]),
                        host_report.split(':')[-1]
                    )
                )
        report.append(drift_report)
    return {
        'report': report,
        'metadata': metadata
    }
