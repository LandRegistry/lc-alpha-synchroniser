#from application.routes import app
from application.utility import encode_name, occupation_string, residences_to_string, get_amendment_text, \
    class_to_numeric, translate_non_pi_name, compare_names
import requests
import json
import kombu
import logging
import re
from datetime import datetime
import traceback


CONFIG = {}


class SynchroniserError(Exception):
    def __init__(self, value):
        self.value = value
        super(SynchroniserError, self).__init__(value)

    def __str__(self):
        return repr(self.value)


def get_eo_party(data):
    if data['class_of_charge'] in ['WOB', 'PAB']:
        lookfor = 'Debtor'
    else:
        lookfor = 'Estate Owner'

    for party in data['parties']:
        if party['type'] == lookfor:
            return party

    raise SynchroniserError("Unable to find EO Name")


def create_legacy_data(data):
    app_type = data['class_of_charge']

    legacy_object = {
        'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
        'registration_no': str(data['registration']['number']).rjust(8),
        'priority_notice': '',  # TODO: Priority Notice
        'registration_date': data['registration']['date'],
        'class_type': app_type,
        'priority_notice_ref': ''
    }

    eo_party = get_eo_party(data)
    if data['class_of_charge'] in ['PAB', 'WOB']:
        legacy_object['address'] = residences_to_string(eo_party)
        legacy_object['property_county'] = ''
        legacy_object['counties'] = ''
        legacy_object['parish_district'] = ''
        legacy_object['property'] = ''
        legacy_object['amendment_info'] = get_amendment_text(data)

    else:
        legacy_object['address'] = ''
        legacy_object['property_county'] = data['particulars']['counties'][0]  # TODO: handle get ID in leg-adapter
        legacy_object['counties'] = data['particulars']['counties'][0]
        legacy_object['parish_district'] = data['particulars']['district']
        legacy_object['property'] = data['particulars']['description']
        legacy_object['amendment_info'] = ''

    # Only sync the top name/county...
    eo_name = eo_party['names'][0]

    if eo_name['type'] == 'Private Individual':
        encoded_name = encode_name(eo_name)
        occupation = occupation_string(eo_party)
        hex_append = ''

    elif eo_name['type'] == 'County Council':
        encoded_name = translate_non_pi_name(eo_name['local']['name'])
        hex_append = "01"
        occupation = ''

    elif eo_name['type'] == 'Parish Council':
        encoded_name = translate_non_pi_name(eo_name['local']['name'])
        hex_append = "04"
        occupation = ''

    elif eo_name['type'] == 'Other Council':
        encoded_name = translate_non_pi_name(eo_name['local']['name'])
        hex_append = "08"
        occupation = ''

    elif eo_name['type'] == 'Development Corporation':
        encoded_name = translate_non_pi_name(eo_name['other'])
        hex_append = "16"
        occupation = ''

    elif eo_name['type'] == 'Limited Company':
        encoded_name = translate_non_pi_name(eo_name['company'])
        hex_append = "F1"
        occupation = ''

    elif data['estate_owner_ind'] == 'Complex Name':
        raise NotImplementedError("Complex Names")

    elif data['estate_owner_ind'] == 'Other':
        encoded_name = translate_non_pi_name(eo_name['other'])
        hex_append = "F2"
        occupation = ''

    else:
        raise SynchroniserError("Unknown name type: {}".format(eo_name['type']))

    legacy_object['reverse_name'] = encoded_name['coded_name']
    legacy_object['remainder_name'] = encoded_name['remainder_name']
    legacy_object['hex_code'] = encoded_name['hex_code']
    legacy_object['occupation'] = occupation
    legacy_object['append_with_hex'] = hex_append
    legacy_object['name'] = encoded_name['name']
    return legacy_object


def get_registration(number, year):
    response = requests.get(CONFIG['REGISTER_URI'] + '/registrations/' + str(number), params={'year': year})
    if response.status_code != 200:
        raise SynchroniserError('/registrations - ' + str(response.status_code))
    return response.json()


def receive_new_regs(body):
    for application in body['data']:

        number = application['number']
        date = application['date']
        logging.info("-----------------------------------------------")
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

            logging.info('PUT ' + json.dumps(converted))
            put_response = requests.put(CONFIG['LEGACY_DB_URI'] + '/land_charges',
                                        data=json.dumps(converted), headers={'Content-Type': 'application/json'})
            if put_response.status_code == 200:
                logging.debug('PUT /land_charges - OK')
            else:
                # TODO: this causes the loop to break. Which is bad.
                raise SynchroniserError("Unexpected response {} on PUT /land_charges for {}/{}".format(
                                        put_response.status_code, number, date))

            coc = class_to_numeric(body['class_of_charge'])
            create_document_row("/{}/{}/{}".format(number, date, coc), number, date, body, 'NR')

            
def create_document_row(resource, reg_no, reg_date, body, app_type):
    doc_row = {
        'class': class_to_numeric(body['class_of_charge']),
        'reg_no': str(reg_no),
        'date': reg_date,
        'orig_class': class_to_numeric(body['class_of_charge']),
        'orig_number': body['registration']['number'],
        'orig_date': body['registration']['date'],
        'canc_ind': '',
        'app_type': app_type
    }
    url = CONFIG['LEGACY_DB_URI'] + '/doc_info' + resource
    
    logging.debug(json.dumps(doc_row))
    put = requests.put(url,
                       data=json.dumps(doc_row),
                       headers={'Content-Type': 'application/json'})
    logging.info('PUT %s - %s', url, str(put.status_code))
    if put.status_code != 200:
        raise SynchroniserError('PUT /doc_info - ' + str(put.status_code))


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
                                          class_to_numeric(regn['class_of_charge']))

            # TODO: consider what happens when cancelling an entry that has pre-existing rows
            # under a different registration number?
            delete = requests.delete(CONFIG['LEGACY_DB_URI'] + '/land_charges' + resource)
            if delete.status_code != 200:
                logging.error('DELETE /land_charges - %s', str(delete.status_code))
                raise SynchroniserError('DELETE /land_charges - ' + str(delete.status_code))
            #create_cancellation_history(body, regn)
            create_document_row(resource, regn['cancellation_ref'], regn['cancellation_date'], regn, 'CN')


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
                                 class_to_numeric(regn['class_of_charge']))
        create_document_row(res, regn['registration']['number'], regn['registration']['date'], oregn, 'AM')


    # Update row on land_charges
    # Write out endorsement
    # Add entry to doc_info (assume changed reg no?)
    pass


def get_entries_for_sync():
    date = datetime.now().strftime('%Y-%m-%d')
    url = CONFIG['REGISTER_URI'] + '/registrations/' + date
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    elif response.status_code != 404:
        raise SynchroniserError("Unexpected response {} from {}".format(response.status_code, url))
    return []


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


def log_stack():
    call_stack = traceback.format_exc()

    lines = call_stack.split("\n")
    for line in lines:
        logging.error(line)
    return call_stack


def raise_error(producer, error):
    producer.put(error)
    logging.warning('Error successfully raised.')
