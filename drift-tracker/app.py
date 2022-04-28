import time

from drift_tracker.report import get_report
from flask import Flask, redirect, render_template, request

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


@app.route("/set-tracking", methods=['GET'])
@app.route("/set-tracking/", methods=['GET'])
def set_tracking_get():
    return render_template('set_tracking.html')


if __name__ == "__main__":
    app.run(host='0.0.0.0')
