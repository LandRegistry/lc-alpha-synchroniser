import sys
from application import app
from application.utility import encode_name, occupation_string, residences_to_string
import requests
import json
import datetime
import logging
import re


class SynchroniserError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


def create_legacy_data(data):
    app_type = re.sub("(.{2})(.*)", '\g<1>(\g<2>)', data['application_type'])
    encoded_debtor_name = encode_name(data['debtor_name'])
    return {
        'time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
        'registration_no': str(data['registration_no']).rjust(8),
        'priority_notice': '',
        'reverse_name': encoded_debtor_name['coded_name'],
        'property_county': 255,  # Always 255 for a bankruptcy.
        'registration_date': data['registration_date'],
        'class_type': app_type,
        'remainder_name': encoded_debtor_name['remainder_name'],
        'punctuation_code': encoded_debtor_name['hex_code'],
        'name': '',
        'address': residences_to_string(data).upper(),
        'occupation': occupation_string(data).upper(),
        'counties': '',
        'amendment_info': 'Insolvency Service Ref. ' + data['application_ref'],  # TODO: somewhat assumed its always INS
        'property': '',
        'parish_district': '',
        'priority_notice_ref': ''
    }


def message_received(body, message):
    logging.info("Received new registrations: {}".format(str(body)))
    errors = []

    request_uri = app.config['REGISTER_URI'] + '/registration/'
    for number in body:
        try:
            logging.debug("Processing {}".format(number))
            uri = request_uri + str(number)
            response = requests.get(uri)

            if response.status_code == 200:
                logging.debug("Received response 200 from /registration")
                data = response.json()
                converted = create_legacy_data(data)
                uri = app.config['LEGACY_DB_URI'] + '/land_charge'
                headers = {'Content-Type': 'application/json'}
                put_response = requests.put(uri, data=json.dumps(converted), headers=headers)
                if put_response.status_code == 200:
                    logging.debug("Received response 200 from /land_charge")
                else:
                    logging.error("Received response {} from /land_charge for registration {}".format(response.status_code,
                                                                                                     number))
                    error = {
                        "uri": '/land_charge',
                        "status_code": put_response.status_code,
                        "message": put_response.content,
                        "registration_no": number
                    }
                    errors.append(error)
            else:
                logging.error("Received response {} from /registration for registration {}".format(response.status_code,
                                                                                                  number))
                error = {
                    "uri": '/registration',
                    "status_code": response.status_code,
                    "registration_no": number
                }
                errors.append(error)
        except Exception as e:
            errors.append({
                "registration_no": number,
                "exception_class": type(e).__name__,
                "error_message": str(e)
            })

    message.ack()
    if len(errors) > 0:
        raise SynchroniserError(errors)




# INTERIM CODE HERE
# This whole having a listener inside the application that issues the errors is just so
# we can do something with them. For Alpha, actually handling the errors isn't being covered.
def error_received(body, message):
    logging.info("Received new error: {}".format(str(body)))
    with open("temp.txt", "a") as file:
        for item in body:
            file.write(json.dumps(item) + "\n")
    message.ack()
    sys.stdout.flush()


def listen(incoming_connection, error_producer, run_forever=True):
    logging.info('Listening for new registrations')

    while True:
        try:
            incoming_connection.drain_events()
        except SynchroniserError as e:
            error_producer.publish(e.value)
            logging.info("Error published")
        except KeyboardInterrupt:
            logging.info("Interrupted")
            break

        if not run_forever:
            break


def listen_for_errors(incoming_connection):
    logging.info('Listening for errors')

    while True:
        try:
            incoming_connection.drain_events()
        except KeyboardInterrupt:
            logging.info("Interrupted")
            break
