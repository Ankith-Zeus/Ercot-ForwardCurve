# wsgi.py
# Loads the Dash app defined in app.py and exposes a Flask "server" for gunicorn
from app import app  # "app" is your Dash() instance in app.py
server = app.server