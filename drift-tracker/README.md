# Drift Tracker
This Flask app shows the drifts of database schema found by `db_drift_checker.py`.
It runs on https://drift-tracker.toolforge.org/.

Categories are the sources of the [table catalog](https://config-master.wikimedia.org/mediawiki-tables.json),
the same types that `db_drift_checker.py` accepts.

## Drift reports
Reports are read from the `drifts/` directory at the root of this repository.
The directory is git-ignored: upload the `drifts_<source>.json` files that
`db_drift_checker.py` writes directly to it, for example:

```
python3 db_drift_checker.py all "sql {wiki} -- " --prod --dc eqiad --output-dir /tmp/drifts
scp /tmp/drifts/drifts_*.json login.toolforge.org:/data/project/drift-tracker/<path to repo>/drifts/
```

`sync_reports.sh` copies the reports from the deployment host to Toolforge,
run it from a machine with ssh access to both.

New files show up on the next request. Set `DRIFTS_DIR` to read them from another directory:

```
export DRIFTS_DIR=/path/to/drifts
```

Tracking (drift code to Phabricator task) is read from
[User:Ladsgroup/drifts.json](https://wikitech.wikimedia.org/wiki/User:Ladsgroup/drifts.json).

## Installing dependencies
This application uses Python 3 and Flask. Using a virtual environment is recommended:

```
# Unix-based systems
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Windows systems
python3 -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

## Starting the application
You can use the `flask` command line utility to start the app:

```
# Unix-based systems
source venv/bin/activate
export FLASK_DEBUG=1
export FLASK_APP=app
flask run

# Windows systems
venv\Scripts\activate
set FLASK_DEBUG=1
set FLASK_APP=app
flask run
```
