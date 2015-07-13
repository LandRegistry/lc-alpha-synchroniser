import sys
from application.utility import encode_name, occupation_string, residences_to_string


def message_received(body, message):
    print("Received Msg: " + str(message))

    encoded_debtor_name = encode_name(body['debtor_name'])
    converted = {
        'time': 'TODO',
        'registration_no': 'TODO',
        'priority_notice': '',
        'reverse_name': encoded_debtor_name['coded_name'],
        'property_county': 255,
        'registration_date': body['date'],
        'class_type': body['application_type'],
        'remainder_name': encoded_debtor_name['remainder_name'],
        'punctuation_code': encoded_debtor_name['hex_code'],
        'name': '',
        'address': residences_to_string(body).upper(),
        'occupation': occupation_string(body).upper(),
        'counties': '',
        'amendment_info': 'Insolvency Service Ref. ' + body['application_ref'], # TODO: somewhat assumed its always INS
        'property': '',
        'parish_district': '',
        'priority_notice_ref': ''
    }

    # TODO: send to legacy-db and handle results
    print(converted)
    message.ack()
    sys.stdout.flush()
