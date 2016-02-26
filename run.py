import config
import importlib
import os
from application.listener import synchronise
from log.logger import setup_logging

cfg = 'config.Config'
c = getattr(importlib.import_module('config'), cfg)
config = {}

for key in dir(c):
    if key.isupper():
        config[key] = getattr(c, key)

setup_logging(config)
synchronise(config)


