import re
import json
import logging


class SynchroniserError(Exception):
    def __init__(self, value):
        self.value = value
        super(SynchroniserError, self).__init__(value)

    def __str__(self):
        return repr(self.value)


def string_encode(text):
    codes_are = {'&': 0, ' ': 1, '-': 2, "'": 3, '(': 4, ')': 5, '*': 6, '?': 7}
    mashed = ""
    codes = ""

    search = re.search(r"['&\s\-\(\)\*\?]", text)
    while search is not None:
        index = search.start()
        word = text[0:index]
        punc = text[index]
        punc = codes_are[punc]

        length = index
        text = text[index + 1:]
        mashed += word
        code = (punc << 5) + length
        codes += '{:02x}'.format(code)

        search = re.search(r"'|\s|\*", text)

    mashed += text

    last_12_chars = mashed[-12:]
    first_chars = mashed[:-12]

    return {
        'coded_name': last_12_chars.upper()[::-1],
        'remainder_name': first_chars.upper(),
        'hex_code': codes.upper(),
        'name': ''
    }


def encode_variant_a_name(text):
    return string_encode(text)


def encode_name(pi_name):
    name = pi_name['private']

    if len(name['forenames']) == 0:  # Special case.
        return encode_variant_a_name(name['surname'])

    logging.info(name)
    mash_with_punc = ' '.join(name['forenames'])
    mash_with_punc += '*' + name['surname']
    return string_encode(mash_with_punc)


def translate_non_pi_name(name):
    no_space = name.replace(" ", "").upper()
    return {
        'coded_name': no_space[:11],
        'remainder_name': no_space[11:],
        'name': name.upper(),
        'hex_code': ''
    }


def get_eo_party(data):
    if data['class_of_charge'] in ['WOB', 'PAB']:
        lookfor = 'Debtor'
    else:
        lookfor = 'Estate Owner'

    for party in data['parties']:
        if party['type'] == lookfor:
            return party

    raise SynchroniserError("Unable to find EO Name")


def address_to_string(address):
    return (' '.join(address['address_lines']) + ' ' + address['postcode'] + ' ' + address['county']).upper()


def get_amendment_text(data):
    court = None
    debtor = None
    for party in data['parties']:
        if party['type'] == 'Court':
            court = party
        if party['type'] == 'Debtor':
            debtor = party

    if court is None:
        logging.warning('No court')
        return debtor['case_reference']

    result = court['names'][0]['other'] + ' ' + debtor['case_reference']
    return result


def residences_to_string(party):
    addresses = ""
    for address in party['addresses']:
        if address['type'] == 'Residence':
            addresses += address_to_string(address) + "   "
    return addresses.strip()


def name_to_string(name):
    result = ' '.join(name['forenames'])
    # if name['middle_names'] != '':
    #    result += ' ' + name['middle_names']
    result += ' ' + name['surname']
    return result


def occupation_string(party):
    # ("(N/A) <AKA foo>+ [T/A <trading name> AS]? <occupation>")
    n_a = "(N/A)"

    names = party['names']
    alias_names = ''
    for name in names[1:]:
        if 'private' not in name:
            raise RuntimeError("Unexpected name data: {}".format(json.dumps(name)))
        alias_names += ' AKA ' + name_to_string(name['private'])

    occu = ''
    if 'trading_name' in party and party['trading_name'] != '':
        occu = " T/A " + party['trading_name']
        if 'occupation' in party:
            occu += " AS " + party['occupation']
    elif 'occupation' in party:
        occu = " " + party['occupation']

    return "{}{}{}".format(n_a, alias_names, occu).upper()


def class_to_numeric(coc):
    classes = {
        'C(I)': 'C1',
        'C(II)': 'C2',
        'C(III)': 'C3',
        'C(IV)': 'C4',
        'D(I)': 'D1',
        'D(II)': 'D2',
        'D(III)': 'D3'
    }

    if coc in classes:
        return classes[coc]
    else:
        return coc


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