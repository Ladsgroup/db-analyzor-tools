import json
import re


def get_tracking_internal():
    try:
        with open('tracking_records.json', 'r') as f:
            return json.loads(f.read())
    except BaseException:
        with open('tracking_records.json', 'w') as f:
            f.write(json.dumps({}))
        return {}


def set_tracking_internal(name, tracking):
    tracking = tracking.strip()
    if not name or not tracking:
        return {
            'error': 'Field is undefined'
        }
    if not re.search(r'^T\d+$', tracking):
        return {
            'error': 'Tracking id is not understandable'
        }
    tracking_values = get_tracking_internal()
    tracking_values[name] = tracking
    with open('tracking_records.json', 'w') as f:
        f.write(json.dumps(tracking_values))
    return {
        'success': 'Done!'
    }
