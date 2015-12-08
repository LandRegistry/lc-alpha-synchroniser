from application.routes import app
from application.utility import encode_name, occupation_string, residences_to_string
import requests
import json
import datetime
import logging
import re


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
        'registration_no': str(data['registration']['number']).rjust(8),
        'priority_notice': '',
        'reverse_name': encoded_debtor_name['coded_name'],
        'property_county': 255,  # Always 255 for a bankruptcy.
                    # TODO: sort out county thing
        'registration_date': data['registration']['date'],
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


def get_registration(number, year):
    response = requests.get(app.config['REGISTER_URI'] + '/registrations/' + str(number), params={'year': year})
    if response.status_code != 200:
        raise SynchroniserError('/registrations - ' + str(response.status_code))
    return response.json()


def create_cancellation_history(body, regn):
    print(body)
    print(regn)
    class_of_charge = re.sub("\(|\)", "", regn['application_type'])
    cancellation = {
        'class': class_of_charge,
        'reg_no': regn['registration']['number'],
        'date': regn['registration']['date'],
        'template': 'Cancellation',
        'text': 'Cancelled by Land Charge reference number(s) {}, on {}'.format(
            regn['cancellation_ref'],
            regn['cancellation_date']  # TODO - needs translation???
        )
    }

    uri = '/history_notes/{}/{}/{}'.format(regn['registration']['number'], regn['cancellation_date'], regn['application_type'])
    response = requests.post(app.config['LEGACY_DB_URI'] + uri, data=json.dumps(cancellation), headers={'Content-Type': 'application/json'})
    logging.info('POST %s - %d', uri, response.status_code)
    return response.status_code


def create_amendment_history(regn):
    class_of_charge = re.sub("\(|\)", "", regn['application_type'])
    template = 'Amend ' + class_of_charge

    amendment = {
        'class': class_of_charge,
        'reg_no': regn['amends_regn']['number'],
        'date': regn['amends_regn']['date'],
        'template': template,
        'text': 'Please note that the registration number %d, dated %s, has been amended by Land Charge '
                'reference number(s) %d, dated %s. Copies of these registrations are enclosed.'.format(
            regn['amends_regn']['number'],
            regn['amends_regn']['date'],
            regn['registration']['number'],
            regn['registration']['date']
        )
    }

    uri = '/history_notes/%s/%s/%s'.format(regn['amends_regn']['number'], regn['amends_regn']['date'], regn['application_type'])
    response = requests.post(uri, data=json.dumps(amendment), headers={'Content-Type': 'application/json'})
    logging.info('POST %s - %s', uri, response.status_code)
    return response.status_code


def receive_new_regs(errors, body):
    for application in body['data']:
        number = application['number']
        date = application['date']
        response = requests.get(app.config['REGISTER_URI'] + '/registrations/' + date + '/' + str(number))
        if response.status_code != 200:
            logging.error("GET /registrations/{} - {}", number, response.status_code)
            errors.append({
                "uri": '/registrations',
                "status_code": response.status_code,
                "registration_no": number
            })
        else:
            logging.debug('Registration retrieved')
            body = response.json()
            converted = create_legacy_data(body)

            put_response = requests.put(app.config['LEGACY_DB_URI'] + '/land_charges',
                                        data=json.dumps(converted), headers={'Content-Type': 'application/json'})
            if put_response.status_code == 200:
                logging.debug('PUT /land_charges - OK')
            else:
                logging.error('PUT /land_charges - {}', put_response.status_code)
                errors.append({
                    "uri": '/land_charge',
                    "status_code": put_response.status_code,
                    "message": put_response.content,
                    "registration_no": number
                })

            coc = body['application_type']
            create_document_row("/{}/{}/{}".format(number, date, coc), number, date, body)


def create_document_row(resource, reg_no, reg_date, body):
    doc_row = {
        'class': body['application_type'],
        'reg_no': reg_no,
        'date': reg_date,
        'orig_class': body['application_type'],
        'orig_no': body['registration']['number'],
        'orig_date': body['registration']['date'],
        'canc_ind': '',
        'app_type': 'CN'
    }
    put = requests.put(app.config['LEGACY_DB_URI'] + '/doc_info' + resource,
                       data=json.dumps(doc_row),
                       headers={'Content-Type': 'application/json'})
    logging.info('POST /doc_info - %s', str(put.status_code))
    if put.status_code != 200:
        raise SynchroniserError('POST /doc_info - ' + str(put.status_code))


def receive_cancellation(errors, body):
    for ref in body['data']:
        number = ref['number']
        date = ref['date']
        response = requests.get(app.config['REGISTER_URI'] + '/registrations/' + date + '/' + str(number))
        if response.status_code != 200:
            logging.error('GET /registrations - %s', str(response.status_code))
            raise SynchroniserError('GET /registrations - ' + str(response.status_code))
        else:
            regn = response.json()
            resource = "/{}/{}/{}".format(regn['registration']['number'],
                                          regn['registration']['date'],
                                          regn['application_type'])
            delete = requests.delete(app.config['LEGACY_DB_URI'] + '/land_charges' + resource)
            if delete.status_code != 200:
                logging.error('DELETE /land_charges - %s', str(delete.status_code))
                raise SynchroniserError('DELETE /land_charges - ' + str(delete.status_code))
            create_cancellation_history(body, regn)
            create_document_row(resource, regn['cancellation_ref'], regn['cancellation_date'], regn)


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
