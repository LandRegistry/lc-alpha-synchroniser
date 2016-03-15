#from application.routes import app
from application.utility import encode_name, occupation_string, residences_to_string, get_amendment_text, \
    class_to_numeric, translate_non_pi_name, compare_names, encode_variant_a_name, get_eo_party, SynchroniserError, \
    class_to_roman
import requests
import json
import kombu
import logging
import re
from datetime import datetime
import traceback


CONFIG = {}


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
    elif data['class_of_charge'] in ['PA', 'WO']:
        legacy_object['address'] = ''
        legacy_object['property_county'] = ""
        legacy_object['counties'] = data['particulars']['counties'][0].upper()
        legacy_object['parish_district'] = data['particulars']['district']
        legacy_object['property'] = data['particulars']['description']
        legacy_object['amendment_info'] = ''
    else:
        legacy_object['address'] = ''
        legacy_object['property_county'] = data['particulars']['counties'][0].upper()
        legacy_object['counties'] = ''
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
        encoded_name = {
            'coded_name': eo_name['search_key'][:11],
            'remainder_name': eo_name['search_key'][12:],
            'name': eo_name['local']['name'].upper(),
            'hex_code': ''
        }
        hex_append = "01"
        occupation = ''

    elif eo_name['type'] == 'Parish Council':
        encoded_name = {
            'coded_name': eo_name['search_key'][:11],
            'remainder_name': eo_name['search_key'][12:],
            'name': eo_name['local']['name'].upper(),
            'hex_code': ''
        }
        hex_append = "04"
        occupation = ''

    elif eo_name['type'] == 'Other Council':
        encoded_name = {
            'coded_name': eo_name['search_key'][:11],
            'remainder_name': eo_name['search_key'][12:],
            'name': eo_name['local']['name'].upper(),
            'hex_code': ''
        }
        hex_append = "08"
        occupation = ''

    elif eo_name['type'] == 'Development Corporation':
        encoded_name = {
            'coded_name': eo_name['search_key'][:11],
            'remainder_name': eo_name['search_key'][12:],
            'name': eo_name['other'].upper(),
            'hex_code': ''
        }
        hex_append = "10"
        occupation = ''

    elif eo_name['type'] == 'Limited Company':
        encoded_name = {
            'coded_name': eo_name['search_key'][:11],
            'remainder_name': eo_name['search_key'][12:],
            'name': eo_name['company'].upper(),
            'hex_code': ''
        }
        hex_append = "F1"
        occupation = ''

    elif eo_name['type'] == 'Complex Name':
        cnum_hex = hex(eo_name['complex']['number'])[2:].zfill(6).upper()
        hex_string = 'F9' + cnum_hex + '00000000000000F3'
        encoded_name = {
            'coded_name': hex_string,
            'remainder_name': '',
            'name': eo_name['complex']['name'].upper(),
            'hex_code': ''
        }
        hex_append = 'F3'
        
    elif eo_name['type'] == 'Coded Name':
        cnum_hex = hex("9999924")[2:].zfill(6).upper()
        hex_string = 'F9' + cnum_hex + '00000000000000F3'
        encoded_name = {
            'coded_name': hex_string,
            'remainder_name': '',
            'name': eo_name['complex']['name'].upper(),
            'hex_code': ''
        }
        hex_append = 'F3'

    elif eo_name['type'] == 'Other':
        if eo_name['subtype'] == 'A':  # VARNAM A
            encoded_name = encode_variant_a_name(eo_name['other'])
            encoded_name['name'] = eo_name['other'].upper()
            hex_append = ""
        else:  # VARNAM B
            encoded_name = {
                'coded_name': eo_name['search_key'][:11],
                'remainder_name': eo_name['search_key'][12:],
                'name': eo_name['other'].upper(),
                'hex_code': ''
            }
            hex_append = "F2"
        occupation = ''

    else:
        raise SynchroniserError("Unknown name type: {}".format(eo_name['type']))

    legacy_object['reverse_name'] = encoded_name['coded_name']
    legacy_object['remainder_name'] = encoded_name['remainder_name']
    legacy_object['punctuation_code'] = encoded_name['hex_code']
    legacy_object['occupation'] = occupation
    legacy_object['append_with_hex'] = hex_append
    legacy_object['name'] = encoded_name['name']
    return legacy_object


def get_registration(number, date):
    url = CONFIG['REGISTER_URI'] + '/registrations/' + date + '/' + str(number)
    response = requests.get(url)
    if response.status_code != 200:
        raise SynchroniserError('Unexpected response {} from {}'.format(response.status_code, url))
    return response.json()


def move_images(number, date, coc):
    uri = '{}/registered_forms/{}/{}'.format(CONFIG['CASEWORK_API_URI'], date, number)
    doc_response = requests.get(uri)
    if doc_response.status_code != 200:
        raise SynchroniserError(uri + ' - ' + str(doc_response.status_code))

    document = doc_response.json()

    uri = '{}/forms/{}'.format(CONFIG['CASEWORK_API_URI'], document['document_id'])
    form_response = requests.get(uri)

    if form_response.status_code != 200:
        raise SynchroniserError(uri + ' - ' + str(form_response.status_code))

    form = form_response.json()
    logging.info('Processing form for %d / %s', number, date)
    for image in form['images']:
        page_number = image['page']
        logging.info("  Page %d", page_number)
        size = image['size']
        uri = '{}/forms/{}/{}?raw=y'.format(CONFIG['CASEWORK_API_URI'], document['document_id'], page_number)
        image_response = requests.get(uri)

        if image_response.status_code != 200:
            raise SynchroniserError(uri + ' - ' + str(image_response.status_code))

        content_type = image_response.headers['Content-Type']
        bin_data = image_response.content

        # Right, now post that to the main database
        class_of_charge = coc
        uri = "{}/images/{}/{}/{}/{}?class={}".format(CONFIG['LEGACY_DB_URI'], date, number, page_number, size, class_of_charge)
        archive_response = requests.put(uri, data=bin_data, headers={'Content-Type': content_type})
        if archive_response.status_code != 200:
            raise SynchroniserError(uri + ' - ' + str(archive_response.status_code))

    # If we've got here, then its on the legacy DB
    uri = '{}/registered_forms/{}/{}'.format(CONFIG['CASEWORK_API_URI'], date, number)
    del_response = requests.delete(uri)
    if del_response.status_code != 200:
        raise SynchroniserError(uri + ' - ' + str(del_response.status_code))


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
            
            move_images(number, date, body['class_of_charge'])

            converted = create_legacy_data(body)

            put_response = create_lc_row(converted)
            if put_response.status_code == 200:
                logging.debug('PUT /land_charges - OK')
            else:
                # TODO: this causes the loop to break. Which is bad.
                raise SynchroniserError("Unexpected response {} on PUT /land_charges for {}/{}".format(
                                        put_response.status_code, number, date))

            coc = class_to_numeric(body['class_of_charge'])
            create_document_row("/{}/{}/{}".format(number, date, coc), number, date, body, 'NR')


def create_lc_row(converted):
    logging.info('PUT ' + json.dumps(converted))
    put_response = requests.put(CONFIG['LEGACY_DB_URI'] + '/land_charges',
                                  data=json.dumps(converted), headers={'Content-Type': 'application/json'})
    return put_response

            
def create_document_row(resource, reg_no, reg_date, body, app_type):
    doc_row = {
        'class': class_to_numeric(body['class_of_charge']),
        'reg_no': str(reg_no),
        'date': reg_date,
        'orig_class': class_to_numeric(body['class_of_charge']),
        'orig_number': body['registration']['number'],
        'orig_date': body['registration']['date'],
        'canc_ind': '',
        'type': app_type
    }
    url = CONFIG['LEGACY_DB_URI'] + '/doc_info' + resource
    
    logging.debug(json.dumps(doc_row))
    put = requests.put(url,
                       data=json.dumps(doc_row),
                       headers={'Content-Type': 'application/json'})
    logging.info('PUT %s - %s', url, str(put.status_code))
    if put.status_code != 200:
        raise SynchroniserError('PUT /doc_info - ' + str(put.status_code))
    return put


def delete_lc_row(number, date, class_of_charge):
    resource = "/{}/{}/{}".format(number, date, class_to_roman(class_of_charge))
    delete = requests.delete(CONFIG['LEGACY_DB_URI'] + '/land_charges' + resource)
    if delete.status_code != 200:
        logging.error('DELETE /land_charges - %s', str(delete.status_code))
        raise SynchroniserError('DELETE /land_charges - ' + str(delete.status_code))


def get_regn_key(regn):
    return regn['number']


def get_full_regn_key(regn):
    return regn['registration']['number']


def get_history(number, date):
    url = CONFIG['REGISTER_URI'] + '/history/' + date + '/' + str(number)
    response = requests.get(url)
    if response.status_code != 200:
        raise SynchroniserError("Unexpected response {} from {}".format(response.status_code, url))
    return json.loads(response.text)


def receive_cancellation(body):

    # Everything in body pertains to *one* detail record, but multiple legacy records
    # However, still assume an image per item

    if len(body['data']) > 0:
        # Iterate through the whole history...
        number = body['data'][0]['number']
        date = body['data'][0]['date']
        history = get_history(number, date)
        original_record = history[-1]  # We'll need this later

        print(original_record)
        original_registrations = []
        for reg in original_record['registrations']:
            oreg = get_registration(reg['number'], reg['date'])
            original_registrations.append(oreg)
            # oreq = requests.get(CONFIG['REGISTER_URI'] + '/registrations/' + reg['date'] + '/' + str(reg['number']))
            # original_registrations.append(json.loads(oreq.text))

        for item in history:
            for reg in item['registrations']:
                delete_lc_row(reg['number'], reg['date'], item['class_of_charge'])

                # resource = "/{}/{}/{}".format(reg['number'], reg['date'], class_to_numeric(item['class_of_charge']))
                # delete = requests.delete(CONFIG['LEGACY_DB_URI'] + '/land_charges' + resource)
                # if delete.status_code != 200:
                #     logging.error('DELETE /land_charges - %s', str(delete.status_code))
                #     raise SynchroniserError('DELETE /land_charges - ' + str(delete.status_code))
    else:
        raise SynchroniserError("Unexpected lack of data for id {}".format(body['id']))

    body['data'] = sorted(body['data'], key=get_regn_key)
    original_registrations = sorted(original_registrations, key=get_full_regn_key)

    if len(original_registrations) != len(body['data']):
        raise SynchroniserError("Unable to process unmatches cancellation lengths")

    for index, ref in enumerate(body['data']):
        number = ref['number']
        date = ref['date']
        registration = get_registration(number, date)
        #     requests.get(CONFIG['REGISTER_URI'] + '/registrations/' + date + '/' + str(number))
        # registration = json.loads(registration.text)
        move_images(number, date, registration['class_of_charge'])

        resource = "/{}/{}/{}".format(registration['registration']['number'],
                                      registration['registration']['date'],
                                      class_to_numeric(registration['class_of_charge']))

        original_registration = original_registrations[index]

        create_document_row(resource, number, date, {
            "class_of_charge": original_registration['class_of_charge'],
            "registration": {
                "number": original_registration['registration']['number'],
                "date": original_registration['registration']['date']
            }
        }, "CN")


def get_amendment_type(new_reg):
    type_of_amend = {
        'Rectification': 'RC',
        'Cancellation': 'CN',
        'Part Cancellation': 'CP',
        'Amendment': 'AM'
    }

    r_code = new_reg['amends_registration']['type']
    if r_code in type_of_amend:
        return type_of_amend[r_code]
    else:
        raise SynchroniserError("Unknown amendment type: {}".format(r_code))


def receive_amendment(body):
    # Get history from the first registration number
    if len(body['data']) == 0:
        raise SynchroniserError("Received empty amendment data list")

    number = body['data'][0]['number']
    date = body['data'][0]['date']
    history = get_history(number, date)

    if len(history) <= 1:  #
        raise SynchroniserError("Received insufficient historical data for amendment")

    current_record = history[0]                         # The first item is the amended (current) record
    amendment_type = current_record['application']      # Get application_type from history[0]['application']
    previous_record = history[1]                        # The second item is the predecessor

    if len(current_record['registrations']) < len(previous_record['registrations']):
        raise SynchroniserError("Unable to handle cancellation implied by len[current] < len[previous]")

    #  Remove the predecessors from leg-land-charge
    for item in previous_record['registrations']:
        delete_lc_row(item['number'], item['date'], previous_record['class_of_charge'])

    for index, item in enumerate(current_record['registrations']):
        move_images(item['number'], item['date'], current_record['class_of_charge'])
        reg = get_registration(item['number'], item['date'])
        converted = create_legacy_data(reg)

        put_response = create_lc_row(converted)
        if put_response.status_code == 200:
            logging.debug('PUT /land_charges - OK')
        else:
            # TODO: this causes the loop to break. Which is bad.
            raise SynchroniserError("Unexpected response {} on PUT /land_charges for {}/{}".format(
                                    put_response.status_code, number, date))
        coc = class_to_numeric(current_record['class_of_charge'])

        if index < len(previous_record['registrations']):
            predecessor = previous_record['registrations'][index]

            res = "/{}/{}/{}".format(item['number'], item['date'], coc)
            a_type = get_amendment_type(reg)
            create_document_row(res, item['number'], item['date'], {
                'class_of_charge': previous_record['class_of_charge'],
                'registration': {
                    'number': predecessor['number'],
                    'date': predecessor['date']
                }
            }, a_type)
        else:
            res = "/{}/{}/{}".format(item['number'], item['date'], coc)
            create_document_row(res, item['number'], item['date'], {
                'class_of_charge': previous_record['class_of_charge'],
                'registration': {
                    'number': item['number'],
                    'date': item['date']
                }
            }, 'NR')

    # for each index, current:
    #   move image
    #   add to leg-land-charge
    #   if current has corresponding pred:
    #       add doc_info (AM)
    #   else
    #       add doc_info (NR)
    pass


    # logging.debug(body)
    # for new_reg in body['data']:
    #     new_get = requests.get(CONFIG['REGISTER_URI'] + '/registrations/' + new_reg['date'] + '/' + str(new_reg['number']))
    #     regn = new_get.json()
    #
    #     logging.debug(regn)
    #     old_reg = {
    #         'number': regn['amends_registration']['number'],
    #         'date': regn['amends_registration']['date']
    #     }
    #
    #     logging.info(old_reg)
    #     logging.info(new_reg)
    #     move_images(regn['registration']['number'], regn['registration']['date'], regn['class_of_charge'])
    #
    #     old_get = requests.get(CONFIG['REGISTER_URI'] + '/registrations/' + old_reg['date'] + '/' + str(old_reg['number']))
    #
    #     oregn = old_get.json()
    #     if not oregn['revealed']:
    #         logging.info('DELETING %d %s', oregn['registration']['number'], oregn['registration']['date'])
    #         requests.delete(CONFIG['LEGACY_DB_URI'] + '/land_charges/{}/{}/{}'.format(oregn['registration']['number'],
    #                                                                                   oregn['registration']['date'],
    #                                                                                   oregn['class_of_charge']))
    #     converted = create_legacy_data(regn)
    #     requests.put(CONFIG['LEGACY_DB_URI'] + '/land_charges',
    #                  data=json.dumps(converted), headers={'Content-Type': 'application/json'})
    #
    #     res = '/{}/{}/{}'.format(regn['registration']['number'],
    #                              regn['registration']['date'],
    #                              class_to_numeric(regn['class_of_charge']))
    #
    #     a_type = get_amendment_type(regn)
    #     create_document_row(res, regn['registration']['number'], regn['registration']['date'], oregn, a_type)

def receive_searches(application):
    # for application in body:
    search_id = application['search_id']
    request_id = application['request_id']

    logging.info("-----------------------------------------------")
    logging.info("Process search id %d", search_id)

    response = requests.get(CONFIG['REGISTER_URI'] + '/request_details/' + str(request_id))
    if response.status_code != 200:
        logging.error("GET /request_details/{} - {}", request_id, response.status_code)
        raise SynchroniserError("Unexpected response {} on GET /request_details/{}".format(
            response.status_code, request_id))
    else:
        logging.debug('Search retrieved')
        body = response.json()
        search_name = body['search_details'][0]['names'][0]
        name = create_search_name(search_name)
        if body['key_number'] == '':
            key_no = ' '
            despatch = body['customer_name'] + '*' + body['customer_address'].replace('\r\n', '*')
        else:
            key_no = body['key_number']
            despatch = ' '

        if body['type'] == 'full':
            form = 'K15'
        else:
            form = 'K16'

        search_data = {'lc_image_part_no': 0,
                       'cust_ref': body['application_reference'].upper(),
                       'desp_name_addr': despatch.upper(),
                       'key_no_cust': key_no,
                       'lc_srch_appn_form': form,
                       'lc_search_name': name
                       }

        uri = '{}/registered_search_forms/{}'.format(CONFIG['CASEWORK_API_URI'], request_id)
        doc_response = requests.get(uri)
        if doc_response.status_code != 200:
            raise SynchroniserError(uri + ' - ' + str(doc_response.status_code))

        document = doc_response.json()

        uri = '{}/forms/{}'.format(CONFIG['CASEWORK_API_URI'], document['document_id'])
        form_response = requests.get(uri)

        if form_response.status_code != 200:
            raise SynchroniserError(uri + ' - ' + str(form_response.status_code))

        form = form_response.json()
        logging.info('Processing form for search %d', search_id)
        for image in form['images']:
            page_number = image['page']
            logging.info("  Page %d", page_number)
            uri = '{}/forms/{}/{}?raw=y'.format(CONFIG['CASEWORK_API_URI'], document['document_id'], page_number)
            image_response = requests.get(uri)

            if image_response.status_code != 200:
                raise SynchroniserError(uri + ' - ' + str(image_response.status_code))

            content_type = image_response.headers['Content-Type']
            bin_data = image_response.content
            search_data['lc_image_part_no'] = page_number
            # search_data['image_data'] = bin_data
            search_data['lc_image_size'] = len(bin_data)
            search_data['lc_image_scan_date'] = datetime.now().strftime('%Y-%m-%d')

            # Right, now post that to the main database
            uri = "{}/search_images".format(CONFIG['LEGACY_DB_URI'])
            archive_response = requests.put(uri, data=json.dumps(search_data), headers={'Content-Type': content_type})
            if archive_response.status_code != 200:
                raise SynchroniserError(uri + ' - ' + str(archive_response.status_code))
            else:
                result = json.loads(archive_response.text)
                search_data['lc_image_id'] = result['lc_image_id']

        # If we've got here, then its on the legacy DB
        uri = '{}/registered_search_forms/{}'.format(CONFIG['CASEWORK_API_URI'], request_id)
        del_response = requests.delete(uri)
        if del_response.status_code != 200:
            raise SynchroniserError(uri + ' - ' + str(del_response.status_code))


def create_search_name(search_name):
    if search_name['type'] == 'Private Individual':
        name = ' '.join(search_name['private']['forenames']) + '*' + search_name['private']['surname']
    elif search_name['type'] == 'Development Corporation':
        name = search_name['other']
    elif search_name['type'] == 'Limited Company':
        name = search_name['company']
    elif search_name['type'] == 'Complex':
        name = str(search_name['complex']['number']) + '*' + search_name['complex']['name']
    elif search_name['type'] == 'Coded Name':
        name = '9999924*' + search_name['other']
    elif search_name['type'] == 'Other':
        name = search_name['other']
    else:
        name_upper = search_name['local']['name'].upper()
        area_upper = search_name['local']['area'].upper()
        replace_area = '+' + area_upper + '+'
        if search_name['type'] == 'County Council':
            name = '01*' + name_upper.replace(area_upper, replace_area)
        elif search_name['type'] == 'Rural Council':
            name = '02*' + name_upper.replace(area_upper, replace_area)
        elif search_name['type'] == 'Parish Council':
            name = '04*' + name_upper.replace(area_upper, replace_area)
        else:  # Other Council
            name = '08*' + name_upper.replace(area_upper, replace_area)

    return name.upper()


def get_entries_for_sync(date):
    logging.info('Get entries for date %s', date)
    url = CONFIG['REGISTER_URI'] + '/registrations/' + date
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    elif response.status_code != 404:
        raise SynchroniserError("Unexpected response {} from {}".format(response.status_code, url))
    return []

    # return []

    # return [{
        # "application": "new",
        # "data": [
            # {
                # "number": 1020,
                # "date": "2016-02-11"
            # }
        # ],
        # "id": 42476878765
    # }]


def get_search_entries_for_sync(date):
    logging.info('Get entries for date %s', date)
    url = CONFIG['REGISTER_URI'] + '/searches/' + date
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    elif response.status_code != 404:
        raise SynchroniserError("Unexpected response {} from {}".format(response.status_code, url))
    return []


def synchronise(config, date):
    global CONFIG
    CONFIG = config

    hostname = "amqp://{}:{}@{}:{}".format(CONFIG['MQ_USERNAME'], CONFIG['MQ_PASSWORD'],
                                           CONFIG['MQ_HOSTNAME'], CONFIG['MQ_PORT'])
    connection = kombu.Connection(hostname=hostname)
    producer = connection.SimpleQueue('errors')

    entries = get_entries_for_sync(date)
    search_entries = get_search_entries_for_sync(date)
    logging.info("Synchroniser starts")
    logging.info("%d Entries received", len(entries))
    logging.info("%d Search Entries received", len(search_entries))

    there_were_errors = False
    for entry in entries:
        logging.info("Process {}".format(entry['application']))

        try:
            if entry['application'] == 'new':
                receive_new_regs(entry)
            elif entry['application'] == 'Cancellation':
                receive_cancellation(entry)
            elif entry['application'] in ['Part Cancellation', 'Rectification', 'Amendment']:
                receive_amendment(entry)
            else:
                raise SynchroniserError('Unknown application type: {}'.format(entry['application']))

        # pylint: disable=broad-except
        except Exception as exception:
            there_were_errors = True
            logging.error('Unhandled error: %s', str(exception))
            s = log_stack()
            raise_error(producer, {
                "message": str(exception),
                "stack": s,
                "subsystem": CONFIG['APPLICATION_NAME'],
                "type": "E"
            })

    for entry in search_entries:
        logging.info("Process {}".format(entry['search_id']))

        try:
            receive_searches(entry)
        except Exception as exception:
            there_were_errors = True
            logging.error('Unhandled error: %s', str(exception))
            s = log_stack()
            raise_error(producer, {
                "message": str(exception),
                "stack": s,
                "subsystem": CONFIG['APPLICATION_NAME'],
                "type": "E"
            })


    logging.info("Synchroniser finishes")
    if there_were_errors:
        logging.error("There were errors")


def log_stack():
    call_stack = traceback.format_exc()

    lines = call_stack.split("\n")
    for line in lines:
        logging.error(line)
    return call_stack


def raise_error(producer, error):
    producer.put(error)
    logging.warning('Error successfully raised.')
