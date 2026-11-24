import re

from flask import Flask, abort, redirect, render_template, request, url_for

from drift_tracker.report import WIDESPREAD, get_report, phabricator_checklist
from drift_tracker.sources import get_categories

app = Flask(__name__)
app.url_map.strict_slashes = False

CATEGORY_RE = re.compile(r'^[a-z0-9_]+$')


@app.context_processor
def inject_categories():
    return {'categories': get_categories()}


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/report')
def report_index():
    return redirect(url_for('report', category='core'))


@app.route('/report/<category>')
def report(category):
    if not CATEGORY_RE.match(category) or category not in get_categories():
        abort(404)
    report_ = get_report(category)
    checklist = None
    if report_ and report_['drifts']:
        url = url_for('report', category=category, _external=True, _scheme='https')
        checklist = phabricator_checklist(category, report_['groups'], url, report_['end_time'])
    return render_template(
        'report.html',
        category=category,
        report=report_,
        checklist=checklist,
        widespread=WIDESPREAD,
        untracked_only=bool(request.args.get('untrackedOnly')),
    )


@app.route('/set-tracking')
def set_tracking():
    return render_template('set_tracking.html')


@app.errorhandler(404)
def page_not_found(e):
    return render_template('page_not_found.html'), 404


if __name__ == '__main__':
    app.run(host='0.0.0.0')
