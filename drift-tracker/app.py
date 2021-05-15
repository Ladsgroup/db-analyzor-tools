from drift_tracker.tracking import set_tracking_internal
from drift_tracker.report import get_report
from flask import Flask, render_template, request

app = Flask(__name__)


valid_categories = [
    'core'
]


@app.route("/")
@app.route("/report/")
@app.route("/report")
def hello():
    return report('core')


@app.route("/report/<category>")
@app.route("/report/<category>/")
def report(category):
    if category not in valid_categories:
        return render_template('page_not_found.html'), 404
    res = get_report(category)
    return render_template('report.html', report=res, len_report=len(res))


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
