# Drift Tracker
This Flask app helps tracking drifts of database schema

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
export FLASK_ENV=development
export FLASK_APP=app
flask run

# Windows systems
venv\Scripts\activate
set FLASK_ENV=development
set FLASK_APP=app
flask run
```
