from collections import OrderedDict

import requests

from .tracking import get_tracking_internal

titles = {
    'field-type-mismatch': 'Mismatching field type of {1}.{0}',
    'field-nullable-mismatch': 'Mismatching field nullability of {1}.{0}',
    'field-unsigned-mismatch': 'Mismatching field unsigned status of {1}.{0}',
    'field-mismatch-codebase-extra': 'Extra field {1}.{0} in code',
    'field-mismatch-prod-extra': 'Extra field {1}.{0} in production',
    'field-size-mismatch': 'Mismatching field size of {1}.{0}',
    'index-mismatch-prod-extra': 'Extra index {1} in production on table {0}',
    'index-mismatch-code-extra': 'Extra index {1} in codebase on table {0}',
}


def get_report(category, untracked_only=False):
    data = requests.get(
        'https://people.wikimedia.org/~ladsgroup/by_drift_type_drifts.json').json()
    tracked = get_tracking_internal()
    data = OrderedDict(
        sorted(data.items(), key=lambda t: len(t[1]), reverse=True))
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
    return report
