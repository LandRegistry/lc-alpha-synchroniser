from flask import Flask, Response
import os
from log.logger import setup_logging
import logging


app = Flask(__name__)
app.config.from_object(os.getenv('SETTINGS', "config.DevelopmentConfig"))

setup_logging(app.config['DEBUG'])


@app.route('/', methods=["GET"])
def root():
    logging.info("GET /")
    return Response(status=200)
