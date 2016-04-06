import pytest
from unittest import mock
import requests
import os
from application.utility import encode_name, address_to_string, residences_to_string
from application.utility import name_to_string, occupation_string, encode_name
from application.sync import SynchroniserError, compare_names
import json

# Can't use data from production system (non-public data); can't tell which pre-prod data is copied from production.
# Data in lower test regions is of unknown reliability.
# Solution: create script in another language to implement the coding rules to create
# this test data. Script checks out OK on pre-prod examples.
test_names = [
    {
        "input": {"private": {"forenames": ["Adelle", "Shaylee"], "surname": "Renner"}},
        "expected": {"coded_name": "RENNEREELYAH", "remainder_name": "ADELLES", "hex_code": "26C7"},
        "string": "Adelle Shaylee Renner"
    }, {
        "input": {"private": {"forenames": ["Bob", "Oscar", "Francis"], "surname": "Howard"}},
        "expected": {"coded_name": "DRAWOHSICNAR", "remainder_name": "BOBOSCARF", "hex_code": "2325C7"},
        "string": "Bob Oscar Francis Howard"
    }, {
        "input": {"private": {"forenames": ["Dominique"], "surname": "O'Brien"}},
        "expected": {"coded_name": "NEIRBOEUQINI", "remainder_name": "DOM", "hex_code": "C961"},
        "string": "Dominique O'Brien"
    }
]

test_address = {
    "input": {"address_lines": ["4144 Considine Burgs", "Hilllton", "West Ima"], "county": "Clwyd", "postcode": "1AA AA1"},
    "expected": "4144 Considine Burgs Hilllton West Ima 1AA AA1 Clwyd".upper()
}

test_residence = {
    "input": {
        "addresses": [
            {"type": "Residence", "address_lines": ["29 Zemlak Street", "East Dallasview"], "postcode": "WK43 2YR", "county": "Fife"},
            {"type": "Residence", "address_lines": ["278 Keaton Estates", "East Hyman"], "postcode": "UY76 5DC", "county": "Cleveland"}
        ]
    },
    "expected": "29 Zemlak Street East Dallasview WK43 2YR Fife   278 Keaton Estates East Hyman UY76 5DC Cleveland".upper()
}

test_occupation = [
    {
        "input": {
            "debtor_names": [],
            "names": [{
                "subtype": "",
                "type": "Private Individual",
                "private": {
                    "forenames": [],
                    "surname": ""
                }
            }],
            "occupation": "Anthropologist"
        },
        "expected": "(N/A) ANTHROPOLOGIST"
    },
    {
        "input": {
            "names": [{
                "subtype": "",
                "type": "Private Individual",
                "private": {
                    "forenames": [],
                    "surname": ""
                }
            }, {
                "subtype": "",
                "type": "Private Individual",
                "private": {
                    "forenames": ["Robert"],
                    "surname": "Howard"
                }
            }],
            "occupation": "Civil Servant"
        },
        "expected": "(N/A) AKA ROBERT HOWARD CIVIL SERVANT"
    },
    {
        "input": {
            "names": [{
                "subtype": "",
                "type": "Private Individual",
                "private": {
                    "forenames": [],
                    "surname": ""
                }
            }, {
                "subtype": "",
                "type": "Private Individual",
                "private": {
                    "forenames": ["Mo"],
                    "surname": "O'Brien"
                }
            }],
            "occupation": "Violinist",
            "trading_name": "Agent Candid"
        },
        "expected": "(N/A) AKA MO O'BRIEN T/A AGENT CANDID AS VIOLINIST"
    }
]

# invalid_data = {
#     "blah": "blah blah blah",
#     "blah blah blah": "blah blah"
# }
#
# application_new_reg = {
#     'application': 'new',
#     'data': [{
#         'number': '50001',
#         'date': '2005-01-01'
#     }]
# }

# cancellation_reg = {
#     'application': 'cancel',
#     'data': [{
#         'number': '50002',
#         'date': '2005-01-01'
#     }]
# }

# amendment_reg = {
#     'application': 'amend',
#     'data': {
#         'new_registrations': [{
#             'number': '50001',
#             'date': '2001-01-01'
#         }],
#         'amended_registrations': [{
#             'number': '50002',
#             'date': '2001-02-01'
#         }]
#     }
# }

# directory = os.path.dirname(__file__)
#
# no_alias = json.loads(open(os.path.join(directory, 'data/50001.json'), 'r').read())
# has_trading = json.loads(open(os.path.join(directory, 'data/50003.json'), 'r').read())
# has_alias = json.loads(open(os.path.join(directory, 'data/50015.json'), 'r').read())
# cancelled = json.loads(open(os.path.join(directory, 'data/50002.json'), 'r').read())
# amended = json.loads(open(os.path.join(directory, 'data/50002.json'), 'r').read())
#
# no_alias_output = json.loads(open(os.path.join(directory, 'data/50001_converted.json'), 'r').read())
# has_trading_output = json.loads(open(os.path.join(directory, 'data/50003_converted.json'), 'r').read())
# has_alias_output = json.loads(open(os.path.join(directory, 'data/50015_converted.json'), 'r').read())


class FakeConnection(object):
    def drain_events(self):
        raise SynchroniserError([{"error_message": "this failed", "exception_class": "Exception"}])


class FakePublisher(object):
    def __init__(self):
        self.data = {}

    def put(self, data):
        self.data = data


class FakeResponse(requests.Response):
    def __init__(self, content=None, status_code=200):
        super(FakeResponse, self).__init__()
        self.data = content
        self.status_code = status_code

    def json(self):
        return self.data


class FakeMessage(object):
    def ack(self):
        pass


# no_alias_resp = FakeResponse(no_alias, status_code=200)
# has_trading_resp = FakeResponse(has_trading, status_code=200)
# has_alias_resp = FakeResponse(has_alias, status_code=200)
# three_errors_resp = FakeResponse({"messages": 3}, status_code=200)
# cancelled_resp = FakeResponse(cancelled)
# amend_resp = FakeResponse(amended)
# exception_data = {"error_message": "this failed", "exception_class": "Exception"}

class TestSynchroniser:
    def test_encode_name_2_forenames(self):
        data = test_names[0]
        encoded = encode_name(data['input'])
        assert encoded['hex_code'] == data['expected']['hex_code']
        assert encoded['remainder_name'] == data['expected']['remainder_name']
        assert encoded['coded_name'] == data['expected']['coded_name']

    def test_encode_name_3_forenames(self):
        data = test_names[1]
        encoded = encode_name(data['input'])
        assert encoded['hex_code'] == data['expected']['hex_code']
        assert encoded['remainder_name'] == data['expected']['remainder_name']
        assert encoded['coded_name'] == data['expected']['coded_name']

    def test_encode_name_apostrophe(self):
        data = test_names[2]
        encoded = encode_name(data['input'])
        assert encoded['hex_code'] == data['expected']['hex_code']
        assert encoded['remainder_name'] == data['expected']['remainder_name']
        assert encoded['coded_name'] == data['expected']['coded_name']

    def test_address_to_string(self):
        output = address_to_string(test_address["input"])
        assert output == test_address["expected"]

    def test_residence_to_string(self):
        output = residences_to_string(test_residence["input"])
        assert output == test_residence["expected"]

    def test_name_to_string_2_forenames(self):
        output = name_to_string(test_names[0]["input"]['private'])
        assert output == test_names[0]["string"]

    def test_name_to_string_3_forenames(self):
        output = name_to_string(test_names[1]["input"]['private'])
        assert output == test_names[1]["string"]

    def test_name_to_string_apostrophe(self):
        output = name_to_string(test_names[2]["input"]['private'])
        assert output == test_names[2]["string"]

    def test_occupation_simple(self):
        output = occupation_string(test_occupation[0]["input"])
        assert output == test_occupation[0]["expected"]

    def test_occupation_alias(self):
        output = occupation_string(test_occupation[1]["input"])
        assert output == test_occupation[1]["expected"]

    def test_occupation_trading(self):
        output = occupation_string(test_occupation[2]["input"])
        assert output == test_occupation[2]["expected"]

    def test_compare_names_ok(self):
        name1 = [{
            'forenames': ['Bob', 'Oscar'],
            'surname': 'Howard'
        }]

        name2 = [{
            'surname': 'Howard',
            'forenames': ['Bob', 'Oscar']
        }]
        assert compare_names(name1, name2) == True

    def test_compare_names_not_ok(self):
        name1 = [{
            'forenames': ['Bob', 'Francis'],
            'surname': 'Howard'
        }]

        name2 = [{
            'surname': 'Howard',
            'forenames': ['Bob', 'Oscar']
        }]
        assert compare_names(name1, name2) == False

    def test_hyphenated_names(self):
        name = {
            "private": {
                "forenames": ["Dave", "Samuel"],
                "surname": "Smith-Smythe"
            }
        }
        coded = encode_name(name)
        assert coded['remainder_name'] == 'DAVESAMUE'
        assert coded['coded_name'] == 'EHTYMSHTIMSL'