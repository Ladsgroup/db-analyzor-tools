import time

from flask import Flask, redirect, render_template, request

from drift_tracker.report import get_report
from drift_tracker.tracking import set_tracking_internal

app = Flask(__name__)


valid_categories = [
    'core'
]


@app.route("/")
@app.route("/report/")
@app.route("/report")
def hello():
    return redirect("/report/core")


@app.route("/report/<category>")
@app.route("/report/<category>/")
def report(category):
    if category not in valid_categories:
        return render_template('page_not_found.html'), 404
    res = get_report(category, request.args.get('untrackedOnly', False))
    report_ = res['report']
    start_time = res['metadata'].get('time_start')
    end_time = res['metadata'].get('time_end')
    duration = None
    if start_time and end_time:
        duration = int(end_time) - int(start_time)
        duration = "{:04.2f}".format(duration/3600)
    if end_time:
        end_time = time.strftime('%d %b %Y %H:%M:%S', time.gmtime(end_time))
    if start_time:
        start_time = time.strftime('%d %b %Y %H:%M:%S', time.gmtime(start_time))
    stats = {
        'total': len(report_),
        'widespread': len([i for i in report_ if i['section_count'] > 5]),
        'untracked': len([i for i in report_ if not i['tracked']]),
        'untracked_widespread': len([i for i in report_ if i['section_count'] > 5 and not i['tracked']]),
    }
    return render_template(
        'report.html', report=report_, stats=stats, duration=duration, start_time=start_time, end_time=end_time)


@app.route("/set-tracking", methods=['POST'])
@app.route("/set-tracking/", methods=['POST'])
def set_tracking():
    name = request.form['name'].strip()
    tracking = request.form['tracking'].strip()
    res = set_tracking_internal(name, tracking)
    return render_template(
        'set_tracking_done.html',
        res=res)


@app.route("/set-tracking", methods=['GET'])
@app.route("/set-tracking/", methods=['GET'])
def set_tracking_get():
    return render_template(
        'set_tracking.html',
        name=request.args.get('name', '').strip(),
        tracking=request.args.get('tracking', '').strip()
    )


if __name__ == "__main__":
    app.run(host='0.0.0.0')
