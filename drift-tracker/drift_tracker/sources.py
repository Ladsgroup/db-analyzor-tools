import json
import os
import time
from functools import wraps
from pathlib import Path

import requests

session = requests.Session()
session.headers['User-Agent'] = 'drift-tracker (https://drift-tracker.toolforge.org/)'

# Reports are uploaded to the git-ignored drifts/ directory of the repository
DEFAULT_DRIFTS_DIR = Path(__file__).resolve().parents[2] / 'drifts'
CATALOG_URL = 'https://config-master.wikimedia.org/mediawiki-tables.json'
TRACKING_URL = 'https://wikitech.wikimedia.org/w/index.php?title=User:Ladsgroup/drifts.json&action=raw'

# Used when the table catalog can not be fetched
FALLBACK_CATEGORIES = ['core']


def ttl_cache(seconds):
    """Cache results per arguments for a fixed time."""
    def decorator(func):
        cache = {}

        @wraps(func)
        def wrapper(*args):
            hit = cache.get(args)
            if hit and hit[0] > time.time():
                return hit[1]
            value = func(*args)
            cache[args] = (time.time() + seconds, value)
            return value
        wrapper.cache_clear = cache.clear
        return wrapper
    return decorator


def drifts_dir():
    return Path(os.environ.get('DRIFTS_DIR', DEFAULT_DRIFTS_DIR))


def get_drifts(category):
    """Drift report of one category, or None if it has not been uploaded."""
    path = drifts_dir() / 'drifts_{}.json'.format(category)
    if not path.is_file():
        return None
    return json.loads(path.read_text())


@ttl_cache(3600)
def get_catalog_sources():
    try:
        res = session.get(CATALOG_URL, timeout=30)
        res.raise_for_status()
        return set(res.json()['sources'])
    except (requests.RequestException, ValueError, KeyError):
        return set(FALLBACK_CATEGORIES)


def get_categories():
    """Sources of the table catalog, the types the drift checker accepts."""
    categories = set(get_catalog_sources())
    # Also show uploaded reports outside the catalog, like 'custom'
    categories |= {
        p.stem[len('drifts_'):] for p in drifts_dir().glob('drifts_*.json')}
    return sorted(categories)


@ttl_cache(300)
def get_tracking():
    """Map of drift code to the Phabricator task that tracks it."""
    try:
        res = session.get(TRACKING_URL, timeout=30)
        res.raise_for_status()
        return res.json()
    except (requests.RequestException, ValueError):
        return {}
