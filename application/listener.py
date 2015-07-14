import sys
from application import settings
from application.utility import encode_name, occupation_string, residences_to_string
import requests
import json
import datetime


def message_received(body, message):
    print(body)

    request_uri = settings['REGISTER_URI'] + '/registration/'
    for number in body:
        uri = request_uri + str(number)
        response = requests.get(uri)
        if response.status_code == 200:
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
                'amendment_info': 'Insolvency Service Ref. ' + data['application_ref'], # TODO: somewhat assumed its always INS
                'property': '',
                'parish_district': '',
                'priority_notice_ref': ''
            }
            print(converted)

            # LEGACY_DB_URI
            uri = settings['LEGACY_DB_URI'] + '/land_charge'
            headers = {'Content-Type': 'application/json'}
            response = requests.put(uri, data=json.dumps(converted), headers=headers)
            print(response.status_code)
            if response.status_code != 200:
                pass  # TODO: error handling


        else:
            pass  # TODO: bucket the error for retrying later






    # url = app.config['B2B_PROCESSOR_URL'] + '/register'
    # headers = {'Content-Type': 'application/json'}
    # response = requests.post(url, data=json.dumps(json_data), headers=headers)



    # encoded_debtor_name = encode_name(body['debtor_name']
    #
    # # TODO: send to legacy-db and handle results
    # print(converted)
    message.ack()
    sys.stdout.flush()
