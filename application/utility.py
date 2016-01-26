import re


def encode_name(name):
    codes_are = {'&': 0, ' ': 1, '-': 2, "'": 3, '(': 4, ')': 5, '*': 6, '?': 7}

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
        'hex_code': codes.upper()
    }


def address_to_string(address):
    # TODO: consider implications of current data being <lines>\<postcode>\<county>,
    # where this is storing <lines>\<county>\<postcode>
    return ' '.join(address['address_lines'])


def residences_to_string(data):
    addresses = ""
    for address in data['residence']:
        addresses += address_to_string(address) + "   "
    return addresses.strip()


def name_to_string(name):
    result = ' '.join(name['forenames'])
    # if name['middle_names'] != '':
    #    result += ' ' + name['middle_names']
    result += ' ' + name['surname']
    return result


def occupation_string(data):
    # ("(N/A) <AKA foo>+ [T/A <trading name> AS]? <occupation>")
    n_a = "(N/A)"
    names = []

    # Should not be able to get this far and have one or the other of these blocks be true
    if 'debtor_names' in data:
        names = data['debtor_names']

    if 'estate_owner' in data and data['estate_owner_ind'] == 'Private Individual':
        names = [data['estate_owner']['private']]

    alias_names = ""
    for name in names:
        alias_names += " " + name_to_string(name)

    if alias_names != "":
        alias_names = " AKA" + alias_names

    if 'trading_name' in data and data['trading_name'] != '':
        occu = " T/A " + data['trading_name'] + " AS " + data['occupation']
    else:
        occu = " " + data['occupation']

    return "{}{}{}".format(n_a, alias_names, occu).upper()
