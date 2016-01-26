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
    app_type = data['class_of_charge']
    encoded_debtor_name = encode_name(data['debtor_names'][0])
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
        'amendment_info': data['legal_body'] + ' ' + data['legal_body_ref'],
        'property': '',
        'parish_district': '',
        'priority_notice_ref': ''
    }

    # if 'lc_register_details' in data:
    # names = [insert_name(cursor, data['lc_register_details']['estate_owner'], party_id)]
    # <option value="privateIndividual">Private individual</option>
    # <option value="limitedCompany">Limited company</option>
    # <option value="localAuthority">Local authority</option>
    # <option value="complexName">Complex name</option>
    # <option value="other">Other</option>


def get_registration(number, year):
    response = requests.get(app.config['REGISTER_URI'] + '/registrations/' + str(number), params={'year': year})
    if response.status_code != 200:
        raise SynchroniserError('/registrations - ' + str(response.status_code))
    return response.json()


def create_cancellation_history(body, regn):
    print(body)
    print(regn)
    class_of_charge = re.sub("\(|\)", "", regn['class_of_charge'])
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

    uri = '/history_notes/{}/{}/{}'.format(regn['registration']['number'], regn['cancellation_date'], regn['class_of_charge'])
    response = requests.post(app.config['LEGACY_DB_URI'] + uri, data=json.dumps(cancellation), headers={'Content-Type': 'application/json'})
    logging.info('POST %s - %d', uri, response.status_code)
    return response.status_code


def create_amendment_history(regn):
    class_of_charge = re.sub("\(|\)", "", regn['class_of_charge'])
    template = 'Amend ' + class_of_charge

    amendment = {
        'class': class_of_charge,
        'reg_no': regn['amends_regn']['number'],
        'date': regn['amends_regn']['date'],
        'template': template,
        'text': 'Please note that the registration number {}, dated {}, has been amended by Land Charge '
                'reference number(s) {}, dated {}. Copies of these registrations are enclosed.'.format(
            regn['amends_regn']['number'],
            regn['amends_regn']['date'],
            regn['registration']['number'],
            regn['registration']['date']
        )
    }

    uri = '/history_notes/{}/{}/{}'.format(regn['amends_regn']['number'], regn['amends_regn']['date'], regn['class_of_charge'])
    response = requests.post(app.config['LEGACY_DB_URI'] + uri, data=json.dumps(amendment), headers={'Content-Type': 'application/json'})
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

            coc = body['class_of_charge']
            create_document_row("/{}/{}/{}".format(number, date, coc), number, date, body, 'NR')


def create_document_row(resource, reg_no, reg_date, body, app_type):
    doc_row = {
        'class': body['class_of_charge'],
        'reg_no': reg_no,
        'date': reg_date,
        'orig_class': body['class_of_charge'],
        'orig_no': body['registration']['number'],
        'orig_date': body['registration']['date'],
        'canc_ind': '',
        'app_type': app_type
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
                                          regn['class_of_charge'])

            # TODO: consider what happens when cancelling an entry that has pre-existing rows
            # under a different registration number?
            delete = requests.delete(app.config['LEGACY_DB_URI'] + '/land_charges' + resource)
            if delete.status_code != 200:
                logging.error('DELETE /land_charges - %s', str(delete.status_code))
                raise SynchroniserError('DELETE /land_charges - ' + str(delete.status_code))
            create_cancellation_history(body, regn)
            create_document_row(resource, regn['cancellation_ref'], regn['cancellation_date'], regn, 'CN')


def compare_names(names1, names2):
    if len(names1) != len(names2):
        return False

    for x in range(len(names1)):
        a = names1[x]
        b = names2[x]
        if len(a['forenames']) != len(b['forenames']):
            return False

        if a['surname'] != b['surname']:
            return False

        for y in range(len(a['forenames'])):
            if a['forenames'][y] != b['forenames'][y]:
                return False

    return True


def receive_amendment(errors, body):
    new_regs = body['data']['new_registrations']
    old_regs = body['data']['amended_registrations']
    if len(new_regs) != len(old_regs):
        logging.error('Length mismatch')
        raise SynchroniserError('Length mismatch')

    for x in range(len(new_regs)):
        old_reg = old_regs[x]
        new_reg = new_regs[x]
        logging.info(old_reg)
        logging.info(new_reg)
        old_get = requests.get(app.config['REGISTER_URI'] + '/registrations/' + old_reg['date'] + '/' + str(old_reg['number']))
        new_get = requests.get(app.config['REGISTER_URI'] + '/registrations/' + new_reg['date'] + '/' + str(new_reg['number']))
        oregn = old_get.json()
        regn = new_get.json()
        # resource = "/{}/{}/{}".format(regn['registration']['number'],
        #                               regn['registration']['date'],
        #                               regn['application_type'])
        # update or replace LC row?
        if compare_names(oregn['debtor_names'], regn['debtor_names']):
            # names match... replace old entry
            logging.info('names match')
            requests.delete(app.config['LEGACY_DB_URI'] + '/land_charges/{}/{}/{}'.format(oregn['registration']['number'],
                                                                                          oregn['registration']['date'],
                                                                                          oregn['class_of_charge']))
        logging.info('HERE')
        converted = create_legacy_data(regn)
        requests.put(app.config['LEGACY_DB_URI'] + '/land_charges',
                     data=json.dumps(converted), headers={'Content-Type': 'application/json'})

        create_amendment_history(regn)
        res = '/{}/{}/{}'.format(regn['registration']['number'],
                                 regn['registration']['date'],
                                 regn['class_of_charge'])
        create_document_row(res, regn['registration']['number'], regn['registration']['date'], oregn, 'AM')


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
            receive_amendment(errors, body)
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
