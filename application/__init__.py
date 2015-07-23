import os
from flask import Flask
import threading


app = Flask(__name__)
app.config.from_object(os.getenv('SETTINGS', "config.DevelopmentConfig"))

from log.logger import setup_logging
setup_logging(app.config['DEBUG'])

from application.server import run, error_run

process_thread = threading.Thread(name='synchroniser', target=run)
process_thread.daemon = True
process_thread.start()

error_thread = threading.Thread(name='synchroniser', target=error_run)
error_thread.daemon = True
error_thread.start()