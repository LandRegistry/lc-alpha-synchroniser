import sys
from application import app
from application.utility import encode_name, occupation_string, residences_to_string
import requests
import json
import datetime
from log.logger import logger


class SynchroniserError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


def message_received(body, message):
    print(type(body))
    logger.info("Received new registrations: {}".format(str(body)))
    errors = []

    request_uri = app.config['REGISTER_URI'] + '/registration/'
    for number in body:
        logger.debug("Processing {}".format(number))
        uri = request_uri + str(number)
        response = requests.get(uri)
        if response.status_code == 200:
            logger.debug("Received response 200 from /registration")
            data = response.json()
            encoded_debtor_name = encode_name(data['debtor_name'])
            converted = {
                'time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
                'registration_no': data['registration_no'],
                'priority_notice': '',
                'reverse_name': encoded_debtor_name['coded_name'],
                'property_county': 255,  # Always 255 for a bankruptcy.
                'registration_date': data['registration_date'],
                'class_type': data['application_type'],
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

            uri = app.config['LEGACY_DB_URI'] + '/land_charge'
            headers = {'Content-Type': 'application/json'}
            put_response = requests.put(uri, data=json.dumps(converted), headers=headers)
            if put_response.status_code == 200:
                logger.debug("Received response 200 from /land_charge")
            else:
                logger.error("Received response {} from /land_charge for registration {}".format(response.status_code,
                                                                                                 number))
                error = {
                    "uri": '/land_charge',
                    "status_code": put_response.status_code,
                    "message": put_response.content,
                    "registration_no": number
                }
                errors.append(error)

        else:
            logger.error("Received response {} from /registration for registration {}".format(response.status_code,
                                                                                              number))
            error = {
                "uri": '/registration',
                "status_code": response.status_code,
                "registration_no": number
            }
            errors.append(error)

    if len(errors) > 0:
        raise SynchroniserError(errors)

    message.ack()
    sys.stdout.flush()


# INTERIM CODE HERE
# This whole having a listener inside the application that issues the errors is just so
# we can do something with them. For Alpha, actually handling the errors isn't being covered.
def error_received(body, message):
    logger.info("Received new error: {}".format(str(body)))
    with open("temp.txt", "a") as file:
        for item in body:
            file.write(json.dumps(item) + "\n")
    message.ack()
    sys.stdout.flush()


def listen(incoming_connection, error_producer):
    logger.info('Listening for new registrations')

    while True:
        try:
            incoming_connection.drain_events()
        except SynchroniserError as e:
            error_producer.publish(e.value)
            logger.info("Error published")
        except KeyboardInterrupt:
            logger.info("Interrupted")
            break


def listen_for_errors(incoming_connection):
    logger.info('Listening for errors')

    while True:
        try:
            incoming_connection.drain_events()
        except KeyboardInterrupt:
            logger.info("Interrupted")
            break
