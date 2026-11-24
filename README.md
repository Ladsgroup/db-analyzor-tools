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

Results are written to `drifts_<source>.json`, in the directory set by
`--output-dir` (default: current directory). Use `--jobs` to set how many
database queries run in parallel (default 8).

Production reports uploaded to the git-ignored `drifts/` directory are shown by
[drift-tracker](https://drift-tracker.toolforge.org/), see
[drift-tracker/README.md](drift-tracker/README.md).

Run tests with `python3 -m unittest discover -s tests -t .`
