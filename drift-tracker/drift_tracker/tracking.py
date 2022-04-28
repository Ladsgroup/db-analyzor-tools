import json

import requests


def get_tracking_internal():
    try:
        text = requests.get(
            'https://wikitech.wikimedia.org/w/index.php?title=User:Ladsgroup/drifts.json&action=raw').text
        return json.loads(text)

    except BaseException:
        return {}