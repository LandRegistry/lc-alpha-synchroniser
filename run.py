import config
import importlib
import logging
from application.sync import synchronise
from log.logger import setup_logging
import sys
from datetime import datetime


reg = None
typ = None
if len(sys.argv) == 2:
    d = sys.argv[1]
elif len(sys.argv) == 5 and sys.argv[1] == '-reg':
    d = sys.argv[3]
    reg = sys.argv[2]
    typ = sys.argv[4]
else:
    d = datetime.now().strftime('%Y-%m-%d')

cfg = 'Config'
c = getattr(importlib.import_module('config'), cfg)
config = {}

for key in dir(c):
    if key.isupper():
        config[key] = getattr(c, key)

setup_logging(config)

excode = '4'
proc, major, minor = (0, 0, 0)
try:
    proc, major, minor = synchronise(config, d, reg_no=reg, appn=typ)

    if major == 0 and minor == 0:
        excode = '0'  # No errors
    elif major == 0:
        excode = '1'  # Only search image failures. Inform devs tomorrow.
    elif major < proc:
        excode = '2'  # Something worked.
    elif major == proc:
        excode = '3'  # It got running but nothing important synched.
except Exception as e:
    logging.error('Failed outside of control loop.')
    logging.error(str(e))
    excode = '4'

logging.info("Registrations processed: %d", proc)
logging.info("Major errors:            %d", major)
logging.info("Minor errors:            %d", minor)

messages = {
    "0": "No errors",
    "1": "Minor errors only",
    "2": "At least one registration failed",
    "3": "All registrations failed",
    "4": "Complete failure"
}
if excode in messages:
    message = messages[excode]
else:
    message = 'Unknown exit code: ' + excode

logging.info('Exit with code %s (%s)', excode, message)
exit(int(excode))
