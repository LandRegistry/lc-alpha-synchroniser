import re
import json
import logging


def encode_name(pi_name):
    codes_are = {'&': 0, ' ': 1, '-': 2, "'": 3, '(': 4, ')': 5, '*': 6, '?': 7}
    name = pi_name['private']
    logging.info(name)
    mash_with_punc = ' '.join(name['forenames'])
    # if name['middle_names'] != '':
    #    mash_with_punc += ' ' + name['middle_names']
    mash_with_punc += '*' + name['surname']

    mashed = ""
    codes = ""

    search = re.search(r"'|\s|\*", mash_with_punc)
    while search is not None:
        index = search.start()
        word = mash_with_punc[0:index]
        punc = mash_with_punc[index]
        punc = codes_are[punc]

        length = index
        mash_with_punc = mash_with_punc[index + 1:]
        mashed += word
        code = (punc << 5) + length
        codes += '{:02x}'.format(code)

        search = re.search(r"'|\s|\*", mash_with_punc)

    mashed += mash_with_punc

    last_12_chars = mashed[-12:]
    first_chars = mashed[:-12]

    return {
        'coded_name': last_12_chars.upper()[::-1],
        'remainder_name': first_chars.upper(),
        'hex_code': codes.upper(),
        'name': ''
    }


def translate_non_pi_name(name):
    no_space = name.replace(" ", "").upper()
    return {
        'coded_name': no_space[:11],
        'remainder_name': no_space[11:],
        'name': name.upper(),
        'hex_code': ''
    }

def address_to_string(address):
    # TODO: consider implications of current data being <lines>\<postcode>\<county>,
    # where this is storing <lines>\<county>\<postcode>
    return ' '.join(address['address_lines'])


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