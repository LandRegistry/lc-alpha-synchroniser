#from application.routes import app
from application.utility import encode_name, occupation_string, residences_to_string, get_amendment_text, \
    class_to_numeric, translate_non_pi_name, compare_names, encode_variant_a_name, get_eo_party, SynchroniserError, \
    class_to_roman
import requests
import json
import kombu
import logging
import getpass
import re
import datetime
import traceback
import urllib.parse


CONFIG = {}
documents_to_delete = []


def get_username():
    return "{}({})".format(
        getpass.getuser(),
        CONFIG['APPLICATION_NAME']
    )


def get_headers(headers=None):
    if headers is None:
        headers = {}

    headers['X-LC-Username'] = get_username()
    return headers


def mark_for_delete(document_id):
    global documents_to_delete
    if document_id not in documents_to_delete:
        documents_to_delete.append(document_id)


# Documents have to be deleted at the end, to account for circumstances where
# one image relates to multiple registrations
def delete_documents():
    for document_id in documents_to_delete:
        uri = "{}/forms/{}".format(CONFIG['CASEWORK_API_URI'], document_id)
        response = requests.delete(uri)
        if response.status_code == 204:
            logging.info("Deleted {}".format(document_id))
        else:
            logging.info("Failed to delete {} -- {}; {}".format(document_id, response.status_code, response.text))


def create_legacy_data(data):
    app_type = class_to_roman(data['class_of_charge'])

    legacy_object = {
        'time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
        'registration_no': str(data['registration']['number']).rjust(8),
        'priority_notice': '',
        'registration_date': data['registration']['date'],
        'class_type': app_type,
        'priority_notice_ref': '',
        'amendment_info': data['additional_information'].upper()[0:254]
    }

    eo_party = get_eo_party(data)

    if data['class_of_charge'] in ['PAB', 'WOB']:
        legacy_object['address'] = residences_to_string(eo_party)
        legacy_object['property_county'] = ''
        legacy_object['counties'] = ''
        legacy_object['parish_district'] = ''
        legacy_object['property'] = ''

    else:
        if 'priority_notice' in data['particulars']:
            legacy_object['priority_notice_ref'] = data['particulars']['priority_notice']

        if 'priority_notice' in data and 'expires' in data['priority_notice']:
            legacy_object['priority_notice'] = 'P'

        legacy_object['address'] = ''
        legacy_object['parish_district'] = data['particulars']['district'].upper()
        legacy_object['property'] = data['particulars']['description'].upper()

        if data['class_of_charge'] in ['PA', 'WO']:
            legacy_object['property_county'] = ""
            legacy_object['counties'] = data['particulars']['counties'][0].upper()

        else:
            county = data['particulars']['counties'][0].upper()
            county = re.sub("(.*) \(CITY OF\)", r"CITY OF \1", county)

            legacy_object['property_county'] = county
            legacy_object['counties'] = ''


    # Only sync the top name/county...
    eo_name = eo_party['names'][0]

    if eo_name['type'] == 'Private Individual':
        encoded_name = encode_name(eo_name)
        occupation = occupation_string(eo_party)[0:254]
        hex_append = ''

    elif eo_name['type'] == 'County Council':
        encoded_name = {
            'coded_name': eo_name['search_key'][:11],
            'remainder_name': eo_name['search_key'][11:],
            'name': eo_name['local']['name'].upper(),
            'hex_code': ''
        }
        hex_append = "01"
        occupation = ''

    elif eo_name['type'] == 'Rural Council':
        encoded_name = {
            'coded_name': eo_name['search_key'][:11],
            'remainder_name': eo_name['search_key'][11:],
            'name': eo_name['local']['name'].upper(),
            'hex_code': ''
        }
        hex_append = "02"
        occupation = ''

    elif eo_name['type'] == 'Parish Council':
        encoded_name = {
            'coded_name': eo_name['search_key'][:11],
            'remainder_name': eo_name['search_key'][11:],
            'name': eo_name['local']['name'].upper(),
            'hex_code': ''
        }
        hex_append = "04"
        occupation = ''

    elif eo_name['type'] == 'Other Council':
        encoded_name = {
            'coded_name': eo_name['search_key'][:11],
            'remainder_name': eo_name['search_key'][11:],
            'name': eo_name['local']['name'].upper(),
            'hex_code': ''
        }
        hex_append = "08"
        occupation = ''

    elif eo_name['type'] == 'Development Corporation':
        encoded_name = {
            'coded_name': eo_name['search_key'][:11],
            'remainder_name': eo_name['search_key'][11:],
            'name': eo_name['other'].upper(),
            'hex_code': ''
        }
        hex_append = "10"
        occupation = ''

    elif eo_name['type'] == 'Limited Company':
        encoded_name = {
            'coded_name': eo_name['search_key'][:11],
            'remainder_name': eo_name['search_key'][11:],
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
        occupation = ''
        
    # elif eo_name['type'] == 'Coded Name':
    #     cnum_hex = hex(9999924)[2:].zfill(6).upper()
    #     hex_string = 'F9' + cnum_hex + '00000000000000F3'
    #     encoded_name = {
    #         'coded_name': hex_string,
    #         'remainder_name': '',
    #         'name': eo_name['other'].upper(),
    #         'hex_code': ''
    #     }
    #     hex_append = 'F3'
    #     occupation = ''

    elif eo_name['type'] == 'Other':
        if eo_name['subtype'] == 'A':  # VARNAM A
            encoded_name = encode_variant_a_name(eo_name['other'])
            encoded_name['name'] = ''  # eo_name['other'].upper()
            hex_append = ""
        else:  # VARNAM B
            encoded_name = {
                'coded_name': eo_name['search_key'][:11],
                'remainder_name': eo_name['search_key'][11:],
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
    response = requests.get(url, headers=get_headers())
    if response.status_code != 200:
        raise SynchroniserError('Unexpected response {} from {}: '.format(response.status_code, url, response.text))
    return response.json()


def move_images(number, date, coc):
    logging.info("Moving image: %d, %s", number, date)
    uri = '{}/registered_forms/{}/{}'.format(CONFIG['CASEWORK_API_URI'], date, number)
    doc_response = requests.get(uri, headers=get_headers())
    if doc_response.status_code == 404:
        # It's quite likely that the documents have already been migrated. This is quite
        # common in test environments.
        logging.warning("No registered forms found for {} of {}".format(number, date))
        return

    if doc_response.status_code != 200:
        raise SynchroniserError(uri + ' - ' + str(doc_response.status_code))

    document = doc_response.json()

    uri = '{}/forms/{}'.format(CONFIG['CASEWORK_API_URI'], document['document_id'])
    form_response = requests.get(uri, headers=get_headers())

    if form_response.status_code != 200:
        raise SynchroniserError(uri + ' - ' + str(form_response.status_code))

    form = form_response.json()
    logging.info('Processing form for %d / %s', number, date)
    for image in form['images']:
        page_number = image['page']
        logging.info("  Page %d", page_number)
        size = image['size']
        uri = '{}/forms/{}/{}?raw=y'.format(CONFIG['CASEWORK_API_URI'], document['document_id'], page_number)
        image_response = requests.get(uri, headers=get_headers())

        if image_response.status_code != 200:
            raise SynchroniserError("Unexpected response from {} - {}: {}".format(uri, image_response.status_code, image_response.text))

        content_type = image_response.headers['Content-Type']
        bin_data = image_response.content

        # Right, now post that to the main database
        class_of_charge = coc
        logging.info('Content-Type: ' + content_type)
        uri = "{}/images/{}/{}/{}/{}?class={}".format(CONFIG['LEGACY_DB_URI'], date, number, page_number, size, class_of_charge)

        headers = get_headers({'Content-Type': content_type})
        logging.info(headers)

        archive_response = requests.put(uri, data=bin_data, headers=headers)
        if archive_response.status_code != 200:
            raise SynchroniserError("Unexpected response from {} - {}: {}".format(uri, archive_response.status_code, archive_response.text))

    mark_for_delete(document['document_id'])

    # If we've got here, then its on the legacy DB
    uri = '{}/registered_forms/{}/{}'.format(CONFIG['CASEWORK_API_URI'], date, number)
    del_response = requests.delete(uri, headers=get_headers())
    if del_response.status_code != 200:
        raise SynchroniserError("Unexpected response from {} - {}: {}".format(uri, del_response.status_code, del_response.text))


def receive_new_regs(body):
    for application in body['data']:

        number = application['number']
        date = application['date']

        logging.info("-----------------------------------------------")
        logging.info("Process registration %d/%s", number, date)

        response = requests.get(CONFIG['REGISTER_URI'] + '/registrations/' + date + '/' + str(number), headers=get_headers())
        if response.status_code != 200:
            logging.error("GET /registrations/{} - {}", number, response.status_code)
            raise SynchroniserError("Unexpected response {} on GET /registrations/{}/{}: ".format(
                                    response.status_code, date, number, response.text))
        else:
            logging.debug('Registration retrieved')
            body = response.json()
            
            move_images(number, date, body['class_of_charge'])

            converted = create_legacy_data(body)

            put_response = create_lc_row(converted)
            if put_response.status_code == 200:
                logging.debug('PUT /land_charges - OK')
            else:
                # TODO: this causes the (inner, new regs) loop to break. Which is bad.
                raise SynchroniserError("Unexpected response {} on PUT /land_charges for {}/{}: ".format(
                                        put_response.status_code, number, date, put_response.text))

            coc = class_to_numeric(body['class_of_charge'])
            create_document_row("/{}/{}/{}".format(number, date, coc), number, date, body, 'NR')


def create_lc_row(converted):
    logging.info('Create LC row for %s %s %s', converted['class_type'],
                 converted['registration_no'].strip(), converted['registration_date'])
    get_resp = requests.get(CONFIG['LEGACY_DB_URI'] + '/land_charges/' + converted['registration_no'].strip(),
                            params={"date": converted['registration_date'], "class": converted['class_type']})

    if get_resp.status_code == 200:
        logging.info("Already an entry on destination. Delete it.")
        #  It exists, delete it...
        del_resp = requests.delete("{}/land_charges/{}/{}/{}".format(
            CONFIG['LEGACY_DB_URI'], converted['registration_no'].strip(),
            converted['registration_date'], converted['class_type']))
        logging.info("Deleting existing row: " + str(del_resp.status_code))

        if del_resp.status_code != 200:
            raise SynchroniserError("Failed to delete record: {}".format(del_resp.text))

    elif get_resp.status_code != 404:
        raise SynchroniserError("Unexpected response for GET /land_charge: {}, {}".format(get_resp.status_code, get_resp.text))

    logging.info('PUT ' + json.dumps(converted))

    put_response = requests.put(CONFIG['LEGACY_DB_URI'] + '/land_charges',
                                data=json.dumps(converted), headers=get_headers({'Content-Type': 'application/json'}))
    logging.info('Response: %d', put_response.status_code)
    return put_response

            
def create_document_row(resource, reg_no, reg_date, body, app_type):
    logging.info('Create document row for %s %d %s', app_type, reg_no, reg_date)
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
                       headers=get_headers({'Content-Type': 'application/json'}))
    logging.info('PUT %s - %s', url, str(put.status_code))
    if put.status_code != 200:
        raise SynchroniserError('PUT /doc_info - ' + str(put.status_code))
    return put


def delete_lc_row(number, date, class_of_charge):
    logging.info('Delete LC row %d %s (%s)', number, date, class_of_charge)
    resource = "/{}/{}/{}".format(number, date, class_to_roman(class_of_charge))
    delete = requests.delete(CONFIG['LEGACY_DB_URI'] + '/land_charges' + resource, headers=get_headers())
    if delete.status_code != 200:
        logging.error('DELETE /land_charges - %s', str(delete.status_code))
        raise SynchroniserError('DELETE /land_charges - ' + str(delete.status_code))


def get_regn_key(regn):
    return regn['number']


def get_full_regn_key(regn):
    return regn['registration']['number']


def get_history(number, date):
    url = CONFIG['REGISTER_URI'] + '/history/' + date + '/' + str(number)
    response = requests.get(url, headers=get_headers())
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

        for item in history:
            for reg in item['registrations']:
                delete_lc_row(reg['number'], reg['date'], item['class_of_charge'])

    else:
        raise SynchroniserError("Unexpected lack of data for id {}".format(body['id']))

    body['data'] = sorted(body['data'], key=get_regn_key)
    original_registrations = sorted(original_registrations, key=get_full_regn_key)

    if len(original_registrations) != len(body['data']):
        raise SynchroniserError("Unable to process unmatched cancellation lengths")

    for index, ref in enumerate(body['data']):
        number = ref['number']
        date = ref['date']
        registration = get_registration(number, date)
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


def has_expired(exp_date):
    if exp_date is None:
        return False

    return datetime.datetime.strptime(exp_date, '%Y-%m-%d').date() <= datetime.date.today()


def pab_amend_case(reg):
    logging.info('Process WOB amendment impacting a PAB')
    # Special case (yay) - need to see if the PAB affected by a WOB amendment should be 'dropped'
    pab_ref = reg['amends_registration']['PAB']
    m = re.match("(\d+)\((\d{4}\-\d+\-\d+)\)", pab_ref) # Nasty, but it's stored in one field
    if m is not None:
        number = m.group(1)
        date = m.group(2)
        pab_reg = get_registration(number, date)

        if has_expired(pab_reg['expired_date']):
            logging.info('Drop associated PAB: %s %s', number, date)
            delete_lc_row(number, date, 'PA(B)')  # Hardcode 'PAB' is OK - it'll fail (good) if somehow a WOB amend
                                                  # has affected anything not a PAB..


def receive_amendment(body, sync_date):
    # Get history from the first registration number
    if len(body['data']) == 0:
        raise SynchroniserError("Received empty amendment data list")

    sync_date = datetime.datetime.strptime(sync_date, '%Y-%m-%d').date()
    number = body['data'][0]['number']
    date = body['data'][0]['date']
    history = get_history(number, date)

    if len(history) <= 1:  #
        raise SynchroniserError("Received insufficient historical data for amendment")

    # current_record = history[0]                         # The first item is the amended (current) record
    # amendment_type = current_record['application']      # Get application_type from history[0]['application']
    # previous_record = history[1]                        # The second item is the predecessor

    for hist_index, item in enumerate(history):

        if len(item['registrations']) < len(history[-1]['registrations']):
            raise SynchroniserError("Unable to handle cancellation implied by len[current] < len[previous]")

        for index, reg_summary in enumerate(item['registrations']):
            reg = get_registration(reg_summary['number'], reg_summary['date'])
            reg_date = datetime.datetime.strptime(reg_summary['date'], '%Y-%m-%d').date()

            if reg_date == sync_date:  # 'today' as far as sync is concerned
                move_images(reg_summary['number'], reg_summary['date'], reg['class_of_charge'])
                if 'amends_registration' in reg and 'PAB' in reg['amends_registration']:
                    pab_amend_case(reg)

                #  Something new; if revealed, make LC row
                if not has_expired(reg['expired_date']):
                    logging.info("%s %d %s is current", reg['class_of_charge'], reg_summary['number'], reg_summary['date'])
                    put_response = create_lc_row(create_legacy_data(reg))
                    if put_response.status_code != 200:
                        raise SynchroniserError("Unexpected response {} on PUT /land_charges for {}/{}".format(
                                                put_response.status_code, number, date))

                #  Create document row regardless, unless a correction
                if reg['amends_registration']['type'] != 'Correction':
                    original = history[-1]
                    if index < len(original['registrations']):
                        predecessor = original['registrations'][index]
                    else:
                        predecessor = original['registrations'][0]

                    coc = class_to_numeric(reg['class_of_charge'])
                    res = "/{}/{}/{}".format(reg_summary['number'], reg_summary['date'], coc)

                    create_document_row(res, reg_summary['number'], reg_summary['date'], {
                        'class_of_charge': original['class_of_charge'],
                        'registration': {'number': predecessor['number'],'date': predecessor['date']}
                    }, get_amendment_type(reg))

            elif reg_date < sync_date:
                #  Something old; if not revealed, remove LC row if it exists
                if has_expired(reg['expired_date']):
                    logging.info("%s %d %s has expired", reg['class_of_charge'], reg_summary['number'], reg_summary['date'])
                    delete_lc_row(reg_summary['number'], reg_summary['date'], reg['class_of_charge'])

                #  If revealed, replace LC row as appropriate (some fields like addl info may change)
                else:
                    put_response = create_lc_row(create_legacy_data(reg))
                    if put_response.status_code != 200:
                        raise SynchroniserError("Unexpected response {} on PUT /land_charges for {}/{}".format(
                                                put_response.status_code, number, date))

                # Documents do not change
            else:
                raise SynchroniserError('Error: synchronising records from the future') # Or being run for a past day


    #
    # if has_expired(previous_record['expired_date']):
    #     for item in previous_record['registrations']:
    #         delete_lc_row(item['number'], item['date'], previous_record['class_of_charge'])
    #
    # for index, item in enumerate(current_record['registrations']):
    #     move_images(item['number'], item['date'], current_record['class_of_charge'])
    #     reg = get_registration(item['number'], item['date'])
    #     logging.info('Synchronise %d %s', item['number'], item['date'])
    #
    #     if 'amends_registration' in reg and 'PAB' in reg['amends_registration']:
    #         logging.info('Process WOB amendment impacting a PAB')
    #         # Special case (yay) - need to see if the PAB affected by a WOB amendment should be 'dropped'
    #         pab_ref = reg['amends_registration']['PAB']
    #         m = re.match("(\d+)\((\d{4}\-\d+\-\d+)\)", pab_ref) # Nasty, but it's stored in one field
    #         if m is not None:
    #             number = m.group(1)
    #             date = m.group(2)
    #             pab_reg = get_registration(number, date)
    #
    #             if has_expired(pab_reg['expired_date']):
    #                 logging.info('Drop associated PAB: %s %s', number, date)
    #                 delete_lc_row(number, date, 'PA(B)')  # Hardcode 'PAB' is OK - it'll fail (good) if somehow a WOB amend
    #                                                       # has affected anything not a PAB...
    #
    #     converted = create_legacy_data(reg)
    #
    #     put_response = create_lc_row(converted)
    #     if put_response.status_code == 200:
    #         logging.debug('PUT /land_charges - OK')
    #     else:
    #         # TODO: this causes the loop to break. Which is bad.
    #         raise SynchroniserError("Unexpected response {} on PUT /land_charges for {}/{}".format(
    #                                 put_response.status_code, number, date))
    #     coc = class_to_numeric(current_record['class_of_charge'])
    #
    #     if index < len(previous_record['registrations']):
    #         predecessor = previous_record['registrations'][index]
    #
    #         res = "/{}/{}/{}".format(item['number'], item['date'], coc)
    #
    #         if reg['amends_registration']['type'] != 'Correction':  # No document row for corrections
    #             a_type = get_amendment_type(reg)
    #             create_document_row(res, item['number'], item['date'], {
    #                 'class_of_charge': previous_record['class_of_charge'],
    #                 'registration': {'number': predecessor['number'],'date': predecessor['date']}
    #             }, a_type)
    #     else:
    #         res = "/{}/{}/{}".format(item['number'], item['date'], coc)
    #         create_document_row(res, item['number'], item['date'], {
    #             'class_of_charge': previous_record['class_of_charge'],
    #             'registration': {'number': item['number'], 'date': item['date']}
    #         }, 'NR')


def receive_searches(application):
    # for application in body:
    search_id = application['search_id']
    request_id = application['request_id']

    logging.info("-----------------------------------------------")
    logging.info("Process search id %d", search_id)

    response = requests.get(CONFIG['REGISTER_URI'] + '/request_details/' + str(request_id), headers=get_headers())
    if response.status_code != 200:
        logging.error("GET /request_details/{} - {}", request_id, response.status_code)
        raise SynchroniserError("Unexpected response {} on GET /request_details/{}".format(
            response.status_code, request_id))
    else:
        logging.debug('Search retrieved')
        body = response.json()
        logging.debug(body)
        search_name = body['search_details'][0]['names'][0]
        name = create_search_name(search_name)
        if body['applicant']['key_number'] == '':
            key_no = ' '
            despatch = body['applicant']['name'] + '*' + body['applicant']['address'].replace('\r\n', '*')
        else:
            key_no = body['applicant']['key_number']
            despatch = ' '

        if body['type'] == 'full':
            form = 'K15'
        else:
            form = 'K16'

        cust_ref = body['applicant']['reference'].upper()
        if cust_ref == '':
            cust_ref = ' '

        cust_ref = cust_ref
        desp_name_addr = despatch.upper()
        key_no_cust = key_no
        lc_srch_appn_form = form
        lc_search_name = name

        uri = '{}/registered_search_forms/{}'.format(CONFIG['CASEWORK_API_URI'], request_id)
        doc_response = requests.get(uri, headers=get_headers())
        if doc_response.status_code == 404:
            logging.warning("Form not found - assume previously synchronised")

        if doc_response.status_code != 200:
            raise SynchroniserError(uri + ' - ' + str(doc_response.status_code))

        document = doc_response.json()

        uri = '{}/forms/{}'.format(CONFIG['CASEWORK_API_URI'], document['document_id'])
        form_response = requests.get(uri, headers=get_headers())

        if form_response.status_code != 200:
            raise SynchroniserError(uri + ' - ' + str(form_response.status_code))

        form = form_response.json()
        logging.info('Processing form for search %d', search_id)
        for image in form['images']:
            page_number = image['page']
            logging.info("  Page %d", page_number)
            uri = '{}/forms/{}/{}?raw=y'.format(CONFIG['CASEWORK_API_URI'], document['document_id'], page_number)
            image_response = requests.get(uri, headers=get_headers())

            if image_response.status_code != 200:
                raise SynchroniserError(uri + ' - ' + str(image_response.status_code))

            content_type = image_response.headers['Content-Type']
            bin_data = image_response.content

            image_size = len(bin_data)
            image_scan_date = datetime.datetime.now().strftime('%Y-%m-%d')

            # Right, now post that to the main database
            data = {
                "cust_ref": cust_ref,
                "desp_name_addr": desp_name_addr,
                "key_no_cust": key_no_cust,
                "lc_srch_appn_form": lc_srch_appn_form,
                "lc_srch_name": lc_search_name,
                "image_size": image_size,
                "image_scan_date": image_scan_date
            }
            uri = "{}/search_images".format(CONFIG['LEGACY_DB_URI'])
            # uri = "{}/search_images/{}/{}/{}/{}/{}/{}/{}".format(CONFIG['LEGACY_DB_URI'], cust_ref, desp_name_addr,
            #                                                      key_no_cust, lc_srch_appn_form, lc_search_name,
            #                                                      image_size, image_scan_date)
            archive_response = requests.put(uri, params={"data": json.dumps(data)}, data=bin_data, headers=get_headers({'Content-Type': content_type}))
            if archive_response.status_code != 200:
                raise SynchroniserError(uri + ' - ' + str(archive_response.status_code))

        # If we've got here, then its on the legacy DB
        uri = '{}/registered_search_forms/{}'.format(CONFIG['CASEWORK_API_URI'], request_id)
        del_response = requests.delete(uri, headers=get_headers())
        if del_response.status_code != 200:
            raise SynchroniserError(uri + ' - ' + str(del_response.status_code))


def create_search_name(search_name):
    if search_name['type'] == 'Private Individual':
        name = ' '.join(search_name['private']['forenames']) + '*' + search_name['private']['surname']
    elif search_name['type'] == 'Development Corporation':
        name = search_name['other']
    elif search_name['type'] == 'Limited Company':
        name = search_name['company']
    elif search_name['type'] == 'Complex Name':
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


def get_entry_for_sync(date, reg_no):
    logging.info('Get entries for %s %s', reg_no, date)
    url = CONFIG['REGISTER_URI'] + '/registrations/' + date + '/' + reg_no
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return [{
            "application": "new",
            "data": [{
                "class_of_charge": data['class_of_charge'],
                "date": date,
                "number": int(reg_no)
            }]
        }]
    else:
        raise SynchroniserError("Unexpected response {} from {}".format(response.status_code, url))


def get_entries_for_sync(date):
    logging.info('Get entries for date %s', date)
    url = CONFIG['REGISTER_URI'] + '/registrations/' + date
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    elif response.status_code != 404:
        raise SynchroniserError("Unexpected response {} from {}".format(response.status_code, url))
    return []


def get_search_entries_for_sync(date):
    logging.info('Get entries for date %s', date)
    url = CONFIG['REGISTER_URI'] + '/searches/' + date
    response = requests.get(url, headers=get_headers())
    if response.status_code == 200:
        return response.json()
    elif response.status_code != 404:
        raise SynchroniserError("Unexpected response {} from {}".format(response.status_code, url))
    return []


def synchronise(config, date, reg_no=None):
    global CONFIG
    CONFIG = config

    hostname = CONFIG['AMQP_URI']
    connection = kombu.Connection(hostname=hostname)
    producer = connection.SimpleQueue('errors')

    if reg_no is None:  # Normal
        entries = get_entries_for_sync(date)
        search_entries = get_search_entries_for_sync(date)
    else:  # For testing only at this time
        entries = get_entry_for_sync(date, reg_no)
        search_entries = []

    logging.info("Synchroniser starts for date %s", date)
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
            elif entry['application'] in ['Part Cancellation', 'Rectification', 'Amendment', 'Renewal', 'Correction']:
                receive_amendment(entry, date)
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

    logging.info("Deleting moved documents...")
    delete_documents()

    logging.info("Synchroniser finishes")
    if there_were_errors:
        logging.error("There were errors")

    return not there_were_errors


def log_stack():
    call_stack = traceback.format_exc()

    lines = call_stack.split("\n")
    for line in lines:
        logging.error(line)
    return call_stack


def raise_error(producer, error):
    producer.put(error)
    logging.warning('Error successfully raised.')
