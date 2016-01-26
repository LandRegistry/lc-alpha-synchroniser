#from application.routes import app
from application.utility import encode_name, occupation_string, residences_to_string
import requests
import json
import datetime
import kombu
import logging
import re
import traceback


CONFIG = {}


class SynchroniserError(Exception):
    def __init__(self, value):
        self.value = value
        super(SynchroniserError, self).__init__(value)

    def __str__(self):
        return repr(self.value)


def create_legacy_data(data):
    app_type = data['class_of_charge']

    legacy_object = {
        'time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
        'registration_no': str(data['registration']['number']).rjust(8),
        'priority_notice': '',
        'property_county': 255,  # Always 255 for a bankruptcy.
                    # TODO: sort out county thing
        'registration_date': data['registration']['date'],
        'class_type': app_type,
        'name': '',
        'address': residences_to_string(data).upper(),
        'counties': '',
        'amendment_info': data['legal_body'] + ' ' + data['legal_body_ref'],
        'property': '',
        'parish_district': '',
        'priority_notice_ref': ''
    }

    # if "class_of_charge" in ['PA(B)', 'WO(B)']:
    if 'debtor_names' in data:  # Simple name, bankruptcy
        encoded_debtor_name = encode_name(data['debtor_names'][0])
        legacy_object['reverse_name'] = encoded_debtor_name['coded_name']
        legacy_object['remainder_name'] = encoded_debtor_name['remainder_name']
        legacy_object['punctuation_code'] = encoded_debtor_name['hex_code']
        legacy_object['occupation'] = occupation_string(data).upper()
        legacy_object['append_with_hex'] = ""

    elif 'estate_owner' in data and data['estate_owner_ind'] == 'Private Individual':
        encoded_debtor_name = encode_name({
            'forenames': data['estate_owner']['private']['forenames'],
            'surname': data['estate_owner']['private']['surname']
        })
        legacy_object['reverse_name'] = encoded_debtor_name['coded_name']
        legacy_object['remainder_name'] = encoded_debtor_name['remainder_name']
        legacy_object['punctuation_code'] = encoded_debtor_name['hex_code']
        legacy_object['occupation'] = occupation_string(data).upper()
        legacy_object['append_with_hex'] = ""

    elif 'estate_owner' in data:
        legacy_object['occupation'] = ""
        legacy_object['punctuation_code'] = ""

        if data['estate_owner_ind'] == 'County Council':
            encoded = translate_non_pi_name(data['estate_owner']['local']['name'])
            legacy_object['append_with_hex'] = "01"

        elif data['estate_owner_ind'] == 'Parish Council':
            encoded = translate_non_pi_name(data['estate_owner']['local']['name'])
            legacy_object['append_with_hex'] = "04"

        elif data['estate_owner_ind'] == 'Other Council':
            encoded = translate_non_pi_name(data['estate_owner']['local']['name'])
            legacy_object['append_with_hex'] = "08"

        elif data['estate_owner_ind'] == 'Development Corporation':
            encoded = translate_non_pi_name(data['estate_owner']['other'])
            legacy_object['append_with_hex'] = "16"

        elif data['estate_owner_ind'] == 'Limited Company':
            encoded = translate_non_pi_name(data['estate_owner']['company'])
            legacy_object['append_with_hex'] = "F1"

        elif data['estate_owner_ind'] == 'Complex Name':
            raise NotImplementedError("Complex Names")

        elif data['estate_owner_ind'] == 'Other':
            encoded = translate_non_pi_name(data['estate_owner']['other'])
            legacy_object['append_with_hex'] = "F2"

        elif data['estate_owner_ind'] == 'Private Individual':
            raise SynchroniserError("How on Earth does the execution get here?")

        else:
            raise SynchroniserError("Unknown estate_owner_ind: {}".format(data['estate_owner_ind']))

        legacy_object['reverse_name'] = encoded['reverse_name']
        legacy_object['remainder_name'] = encoded['remainder']
        legacy_object['name'] = encoded['name']

    return legacy_object


def translate_non_pi_name(name):
    no_space = name.replace(" ", "").upper()
    return {
        'reverse_name': no_space[:11],
        'remainder': no_space[11:],
        'name': name.upper()
    }


def get_registration(number, year):
    response = requests.get(CONFIG['REGISTER_URI'] + '/registrations/' + str(number), params={'year': year})
    if response.status_code != 200:
        raise SynchroniserError('/registrations - ' + str(response.status_code))
    return response.json()


# def create_cancellation_history(body, regn):
#     print(body)
#     print(regn)
#     class_of_charge = re.sub("\(|\)", "", regn['class_of_charge'])
#     cancellation = {
#         'class': class_of_charge,
#         'reg_no': regn['registration']['number'],
#         'date': regn['registration']['date'],
#         'template': 'Cancellation',
#         'text': 'Cancelled by Land Charge reference number(s) {}, on {}'.format(
#             regn['cancellation_ref'],
#             regn['cancellation_date']  # TODO - needs translation???
#         )
#     }
#
#     uri = '/history_notes/{}/{}/{}'.format(regn['registration']['number'], regn['cancellation_date'], regn['class_of_charge'])
#     response = requests.post(CONFIG['LEGACY_DB_URI'] + uri, data=json.dumps(cancellation), headers={'Content-Type': 'application/json'})
#     logging.info('POST %s - %d', uri, response.status_code)
#     return response.status_code


# def create_amendment_history(regn):
#     class_of_charge = re.sub("\(|\)", "", regn['class_of_charge'])
#     template = 'Amend ' + class_of_charge
#
#     amendment = {
#         'class': class_of_charge,
#         'reg_no': regn['amends_regn']['number'],
#         'date': regn['amends_regn']['date'],
#         'template': template,
#         'text': 'Please note that the registration number {}, dated {}, has been amended by Land Charge '
#                 'reference number(s) {}, dated {}. Copies of these registrations are enclosed.'.format(
#             regn['amends_regn']['number'],
#             regn['amends_regn']['date'],
#             regn['registration']['number'],
#             regn['registration']['date']
#         )
#     }
#
#     uri = '/history_notes/{}/{}/{}'.format(regn['amends_regn']['number'], regn['amends_regn']['date'], regn['class_of_charge'])
#     response = requests.post(CONFIG['LEGACY_DB_URI'] + uri, data=json.dumps(amendment), headers={'Content-Type': 'application/json'})
#     logging.info('POST %s - %s', uri, response.status_code)
#     return response.status_code


def receive_new_regs(body):
    for application in body['data']:
        number = application['number']
        date = application['date']
        logging.info("Process registration %d/%s", number, date)

        response = requests.get(CONFIG['REGISTER_URI'] + '/registrations/' + date + '/' + str(number))
        if response.status_code != 200:
            logging.error("GET /registrations/{} - {}", number, response.status_code)
            raise SynchroniserError("Unexpected response {} on GET /registrations/{}/{}".format(
                                    response.status_code, date, number))
        else:
            logging.debug('Registration retrieved')
            body = response.json()
            converted = create_legacy_data(body)

            put_response = requests.put(CONFIG['LEGACY_DB_URI'] + '/land_charges',
                                        data=json.dumps(converted), headers={'Content-Type': 'application/json'})
            if put_response.status_code == 200:
                logging.debug('PUT /land_charges - OK')
            else:
                raise SynchroniserError("Unexpected response {} on PUT /land_charges for {}/{}".format(
                                        put_response.status_code, number, date))

            coc = body['class_of_charge']
            create_document_row("/{}/{}/{}".format(number, date, coc), number, date, body, 'NR')


def create_document_row(resource, reg_no, reg_date, body, app_type):
    doc_row = {
        'class': body['class_of_charge'],
        'reg_no': str(reg_no),
        'date': reg_date,
        'orig_class': body['class_of_charge'],
        'orig_no': body['registration']['number'],
        'orig_date': body['registration']['date'],
        'canc_ind': '',
        'app_type': app_type
    }
    url = CONFIG['LEGACY_DB_URI'] + '/doc_info' + resource
    put = requests.put(url,
                       data=json.dumps(doc_row),
                       headers={'Content-Type': 'application/json'})
    logging.info('PUT %s - %s', url, str(put.status_code))
    if put.status_code != 200:
        raise SynchroniserError('POST /doc_info - ' + str(put.status_code))


def receive_cancellation(body):
    for ref in body['data']:
        number = ref['number']
        date = ref['date']
        response = requests.get(CONFIG['REGISTER_URI'] + '/registrations/' + date + '/' + str(number))
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
            delete = requests.delete(CONFIG['LEGACY_DB_URI'] + '/land_charges' + resource)
            if delete.status_code != 200:
                logging.error('DELETE /land_charges - %s', str(delete.status_code))
                raise SynchroniserError('DELETE /land_charges - ' + str(delete.status_code))
            #create_cancellation_history(body, regn)
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


def receive_amendment(body):
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
        old_get = requests.get(CONFIG['REGISTER_URI'] + '/registrations/' + old_reg['date'] + '/' + str(old_reg['number']))
        new_get = requests.get(CONFIG['REGISTER_URI'] + '/registrations/' + new_reg['date'] + '/' + str(new_reg['number']))
        oregn = old_get.json()
        regn = new_get.json()
        # resource = "/{}/{}/{}".format(regn['registration']['number'],
        #                               regn['registration']['date'],
        #                               regn['application_type'])
        # update or replace LC row?
        if compare_names(oregn['debtor_names'], regn['debtor_names']):
            # names match... replace old entry
            logging.info('names match')
            requests.delete(CONFIG['LEGACY_DB_URI'] + '/land_charges/{}/{}/{}'.format(oregn['registration']['number'],
                                                                                      oregn['registration']['date'],
                                                                                      oregn['class_of_charge']))
        logging.info('HERE')
        converted = create_legacy_data(regn)
        requests.put(CONFIG['LEGACY_DB_URI'] + '/land_charges',
                     data=json.dumps(converted), headers={'Content-Type': 'application/json'})

        #create_amendment_history(regn)
        res = '/{}/{}/{}'.format(regn['registration']['number'],
                                 regn['registration']['date'],
                                 regn['class_of_charge'])
        create_document_row(res, regn['registration']['number'], regn['registration']['date'], oregn, 'AM')


    # Update row on land_charges
    # Write out endorsement
    # Add entry to doc_info (assume changed reg no?)
    pass


def get_entries_for_sync():
    return [{
        'application': 'new',
        'data': [
            {'number': 1002, 'date': '2016-01-26', 'county': 'Devon'},
            {'number': 1003, 'date': '2016-01-26', 'county': 'Buckinghamshire'}
        ]
    }, {
        'application': 'new',
        "data": [{
            "county": "Devon",
            "number": 1000,
            "date": "2016-01-26"
        },
        {
            "county": "Buckinghamshire",
            "number": 1001,
            "date": "2016-01-26"
        }
        ],
        "request_id": 573
    }]
        # ,
        # {
        # 'application': 'new',
        # 'data': [{
        #     "surname": "Howard",
        #     "forenames": ["Bob", "Oscar", "Francis"],
        #     "date": "2016-01-01",
        #     "number": 1010
        # }, {
        #     "surname": "Howard",
        #     "forenames": ["Robert"],
        #     "date": "2016-01-01",
        #     "number": 1011
        # }]
    #}]


def synchronise(config):
    global CONFIG
    CONFIG = config

    hostname = "amqp://{}:{}@{}:{}".format(CONFIG['MQ_USERNAME'], CONFIG['MQ_PASSWORD'],
                                           CONFIG['MQ_HOSTNAME'], CONFIG['MQ_PORT'])
    connection = kombu.Connection(hostname=hostname)
    producer = connection.SimpleQueue('errors')

    entries = get_entries_for_sync()
    logging.info("Synchroniser starts")
    logging.info("Entries received")

    for entry in entries:
        logging.info("Process {}".format(entry['application']))
        try:
            if entry['application'] == 'new':
                receive_new_regs(entry)
            elif entry['application'] == 'cancel':
                receive_cancellation(entry)
            elif entry['application'] == 'amend':
                receive_amendment(entry)
            else:
                raise SynchroniserError('Unknown application type: %s', entry['application'])

        # pylint: disable=broad-except
        except Exception as exception:
            logging.error('Unhandled error: %s', str(exception))
            s = log_stack()
            raise_error(producer, {
                "message": str(exception),
                "stack": s,
                "subsystem": CONFIG['APPLICATION_NAME'],
                "type": "E"
            })
    logging.info("Synchroniser finishes")

# def message_received(body, message):
#     logging.info("Received new message: %s", str(body))
#
#     errors = []
#     try:
#         if body['application'] == 'new':
#             receive_new_regs(errors, body)
#         elif body['application'] == 'cancel':
#             receive_cancellation(errors, body)
#         elif body['application'] == 'amend':
#             receive_amendment(errors, body)
#         else:
#             logging.error('Unknown application type: %s', body['application'])
#             errors.append({
#                 'message': 'Unknown application type {}'.format(body['application'])
#             })
#     # pylint: disable=broad-except
#     except Exception as exception:
#         logging.error('Unhandled error: %s', str(exception))
#         s = log_stack()
#         errors.append({
#             "message": str(exception),
#             "stack": s,
#             "subsystem": CONFIG['APPLICATION_NAME'],
#             "type": "E"
#             # "message": body,
#             # "exception_class": type(exception).__name__,
#             # "error_message": str(exception)
#         })
#
#     message.ack()
#     if len(errors) > 0:
#         raise SynchroniserError(errors)


def log_stack():
    call_stack = traceback.format_exc()

    lines = call_stack.split("\n")
    for line in lines:
        logging.error(line)
    return call_stack


def raise_error(producer, error):
    producer.put(error)
    logging.warning('Error successfully raised.')


# def listen(incoming_connection, error_producer, run_forever=True):
#     logging.info('Listening for new registrations')
#
#     while True:
#         try:
#             incoming_connection.drain_events()
#         except SynchroniserError as exception:
#             for error in exception.value:
#                 error_producer.put(error)
#             logging.info("Error published")
#         except KeyboardInterrupt:
#             logging.info("Interrupted")
#             break
#
#         if not run_forever:
#             break
