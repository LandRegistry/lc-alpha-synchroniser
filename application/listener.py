from application.routes import app
from application.utility import encode_name, occupation_string, residences_to_string
import requests
import json
import datetime
import logging


class SynchroniserError(Exception):
    def __init__(self, value):
        self.value = value
        super(SynchroniserError, self).__init__(value)

    def __str__(self):
        return repr(self.value)


def create_legacy_data(data):
    app_type = data['application_type']
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


def receive_new_regs(errors, body):
    for number in body['data']:
        response = requests.get(app.config['REGISTER_URI'] + '/registrations/' + str(number))
        if response.status_code != 200:
            logging.error("GET /registrations/%d - %s", number, response.status_code)
            errors.append({
                "uri": '/registrations',
                "status_code": response.status_code,
                "registration_no": number
            })
        else:
            logging.debug('Registration retrieved')
            converted = create_legacy_data(response.json())

            put_response = requests.put(app.config['LEGACY_DB_URI'] + '/land_charges',
                                        data=json.dumps(converted), headers={'Content-Type': 'application/json'})
            if put_response.status_code == 200:
                logging.debug('PUT /land_charges - OK')
            else:
                logging.error('PUT /land_charges - %s', put_response.status_code)
                errors.append({
                    "uri": '/land_charge',
                    "status_code": put_response.status_code,
                    "message": put_response.content,
                    "registration_no": number
                })


def receive_cancellation(errors, body):
    # Delete from from land_charges
    # Write out endorsement
    # Add entry to doc_info
    pass


def receive_update(errors, body):
    # Update row on land_charges
    # Write out endorsement
    # Add entry to doc_info (assume changed reg no?)
    pass


def message_received(body, message):
    logging.info("Received new message: %s", str(body))

    errors = []
    try:
        if body['application'] == 'new':
            receive_new_regs(errors, body)
        elif body['application'] == 'cancel':
            receive_cancellation(errors, body)
        elif body['application'] == 'amend':
            receive_update(errors, body)
        else:
            logging.error('Unknown application type: %s', body['application'])
            errors.append({
                'message': 'Unknown application type {}'.format(body['application'])
            })
    # pylint: disable=broad-except
    except Exception as exception:
        logging.error('Unhandled error: %s', str(exception))
        errors.append({
            "message": body,
            "exception_class": type(exception).__name__,
            "error_message": str(exception)
        })

    message.ack()
    if len(errors) > 0:
        raise SynchroniserError(errors)

    #
    # request_uri = app.config['REGISTER_URI'] + '/registrations/'
    # for number in body:
    #     try:
    #         logging.debug("Processing %s", number)
    #         uri = request_uri + str(number)
    #         response = requests.get(uri)
    #
    #         if response.status_code == 200:
    #             logging.debug("Received response 200 from /registrations")
    #             data = response.json()
    #             converted = create_legacy_data(data)
    #             uri = app.config['LEGACY_DB_URI'] + '/land_charges'
    #             headers = {'Content-Type': 'application/json'}
    #             put_response = requests.put(uri, data=json.dumps(converted), headers=headers)
    #             if put_response.status_code == 200:
    #                 logging.debug("Received response 200 from /land_charges")
    #             else:
    #                 logging.error("Received response %d from /land_charges for registration %s",
    #                               response.status_code, number)
    #                 error = {
    #                     "uri": '/land_charge',
    #                     "status_code": put_response.status_code,
    #                     "message": put_response.content,
    #                     "registration_no": number
    #                 }
    #                 errors.append(error)
    #         else:
    #             logging.error("Received response %d from /registrations for registration %s",
    #                           response.status_code, number)
    #             error = {
    #                 "uri": '/registrations',
    #                 "status_code": response.status_code,
    #                 "registration_no": number
    #             }
    #             errors.append(error)
    #     # pylint: disable=broad-except
    #     except Exception as exception:
    #         errors.append({
    #             "registration_no": number,
    #             "exception_class": type(exception).__name__,
    #             "error_message": str(exception)
    #         })
    #
    # message.ack()
    # if len(errors) > 0:
    #     raise SynchroniserError(errors)


def listen(incoming_connection, error_producer, run_forever=True):
    logging.info('Listening for new registrations')

    while True:
        try:
            incoming_connection.drain_events()
        except SynchroniserError as exception:
            for error in exception.value:
                error_producer.put(error)
            logging.info("Error published")
        except KeyboardInterrupt:
            logging.info("Interrupted")
            break

        if not run_forever:
            break
