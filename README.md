# db-analyzor-tools
Python tools to analyze databases, like finding drifts or abstracting sql

## db_drift_checker.py
Compares the abstract schema of MediaWiki tables with the actual databases.
Schema files and table placement (sections, dblists, wikis) come from the
[table catalog](https://config-master.wikimedia.org/mediawiki-tables.json).

```
# One source of the catalog (core, wikibase_client, flaggedrevs, ...), or "all"
python3 db_drift_checker.py core "sql {wiki} -- " --prod --dc eqiad
# Local database with a custom schema file
python3 db_drift_checker.py custom "sudo mysql " --wiki my_wiki \
    --gerrit-schema-file mediawiki/core/+/REL1_43/sql/tables.json
```

Results are written to `drifts_<source>.json`. Use `--jobs` to set how many
database queries run in parallel (default 8).

Run tests with `python3 -m unittest discover -s tests -t .`
