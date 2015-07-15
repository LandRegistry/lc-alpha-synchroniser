import pytest
from unittest import mock
import requests
import os
from application import app
from application.utility import encode_name, address_to_string, residences_to_string
from application.utility import name_to_string, occupation_string
from application.listener import message_received, SynchroniserError
import json

# Can't use data from production system (non-public data); can't tell which pre-prod data is copied from production.
# Data in lower test regions is of unknown reliability.
# Solution: create script in another language to implement the coding rules to create
# this test data. Script checks out OK on pre-prod examples.
test_names = [
    {
        "input": {"forename": "Adelle", "middle_names": "Shaylee", "surname": "Renner"},
        "expected": {"coded_name": "RENNEREELYAH", "remainder_name": "ADELLES", "hex_code": "26C7"},
        "string": "Adelle Shaylee Renner"
    }, {
        "input": {"forename": "Bob", "middle_names": "Oscar Francis", "surname": "Howard"},
        "expected": {"coded_name": "DRAWOHSICNAR", "remainder_name": "BOBOSCARF", "hex_code": "2325C7"},
        "string": "Bob Oscar Francis Howard"
    }, {
        "input": {"forename": "Dominique", "middle_names": "", "surname": "O'Brien"},
        "expected": {"coded_name": "NEIRBOEUQINI", "remainder_name": "DOM", "hex_code": "C961"},
        "string": "Dominique O'Brien"
    }
]

test_address = {
    "input": {"address_lines": ["4144 Considine Burgs", "Hilllton", "West Ima", "Clwyd"]},
    "expected": "4144 Considine Burgs Hilllton West Ima Clwyd"
}

test_residence = {
    "input": {
        "residence": [
            {"address_lines": ["29 Zemlak Street", "East Dallasview", "Fife", "WK43 2YR"]},
            {"address_lines": ["278 Keaton Estates", "East Hyman", "Cleveland", "UY76 5DC"]}
        ]
    },
    "expected": "29 Zemlak Street East Dallasview Fife WK43 2YR   278 Keaton Estates East Hyman Cleveland UY76 5DC"
}

test_occupation = [
    {
        "input": {
            "debtor_alias": [],
            "occupation": "Anthropologist"
        },
        "expected": "(N/A) ANTHROPOLOGIST"
    },
    {
        "input": {
            "debtor_alias": [{"forename": "Robert", "middle_names": "", "surname": "Howard"}],
            "occupation": "Civil Servant"
        },
        "expected": "(N/A) ROBERT HOWARD CIVIL SERVANT"
    },
    {
        "input": {
            "debtor_alias": [{"forename": "Mo", "middle_names": "", "surname": "O'Brien"}],
            "occupation": "Violinist",
            "trading_name": "Agent Candid"
        },
        "expected": "(N/A) MO O'BRIEN T/A AGENT CANDID AS VIOLINIST"
    }
]

directory = os.path.dirname(__file__)

no_alias = json.loads(open(os.path.join(directory, 'data/50001.json'), 'r').read())
has_trading = json.loads(open(os.path.join(directory, 'data/50003.json'), 'r').read())
has_alias = json.loads(open(os.path.join(directory, 'data/50015.json'), 'r').read())

no_alias_output = json.loads(open(os.path.join(directory, 'data/50001_converted.json'), 'r').read())
has_trading_output = json.loads(open(os.path.join(directory, 'data/50003_converted.json'), 'r').read())
has_alias_output = json.loads(open(os.path.join(directory, 'data/50015_converted.json'), 'r').read())


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

no_alias_resp = FakeResponse(no_alias, status_code=200)
has_trading_resp = FakeResponse(has_trading, status_code=200)
has_alias_resp = FakeResponse(has_alias, status_code=200)
three_errors_resp = FakeResponse({"messages": 3}, status_code=200)


class TestSynchroniser:
    def setup_method(self, method):
        self.app = app.test_client()

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
        output = name_to_string(test_names[0]["input"])
        assert output == test_names[0]["string"]

    def test_name_to_string_3_forenames(self):
        output = name_to_string(test_names[1]["input"])
        assert output == test_names[1]["string"]

    def test_name_to_string_apostrophe(self):
        output = name_to_string(test_names[2]["input"])
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

    @mock.patch('requests.get', return_value=no_alias_resp)
    @mock.patch('requests.put', return_value=FakeResponse())
    def test_convert_no_alias(self, mock_put, mock_get):
        message_received([50000], FakeMessage())
        onward_data = json.loads(mock_put.call_args[1]['data'])
        assert onward_data['punctuation_code'] == no_alias_output['punctuation_code']
        assert onward_data['address'] == no_alias_output['address']
        assert onward_data['reverse_name'] == no_alias_output['reverse_name']
        assert onward_data['occupation'] == no_alias_output['occupation']
        assert onward_data['remainder_name'] == no_alias_output['remainder_name']

    @mock.patch('requests.get', return_value=has_alias_resp)
    @mock.patch('requests.put', return_value=FakeResponse())
    def test_convert_no_alias(self, mock_put, mock_get):
        message_received([50000], FakeMessage())
        onward_data = json.loads(mock_put.call_args[1]['data'])
        assert onward_data['punctuation_code'] == has_alias_output['punctuation_code']
        assert onward_data['address'] == has_alias_output['address']
        assert onward_data['reverse_name'] == has_alias_output['reverse_name']
        assert onward_data['occupation'] == has_alias_output['occupation']
        assert onward_data['remainder_name'] == has_alias_output['remainder_name']

    @mock.patch('requests.get', return_value=has_trading_resp)
    @mock.patch('requests.put', return_value=FakeResponse())
    def test_convert_no_alias(self, mock_put, mock_get):
        message_received([50000], FakeMessage())
        onward_data = json.loads(mock_put.call_args[1]['data'])
        assert onward_data['punctuation_code'] == has_trading_output['punctuation_code']
        assert onward_data['address'] == has_trading_output['address']
        assert onward_data['reverse_name'] == has_trading_output['reverse_name']
        assert onward_data['occupation'] == has_trading_output['occupation']
        assert onward_data['remainder_name'] == has_trading_output['remainder_name']

    @mock.patch('requests.get', return_value=FakeResponse(status_code=500))
    @mock.patch('requests.put', return_value=FakeResponse())
    def test_error_get_failed(self, mock_put, mock_get):
        with pytest.raises(SynchroniserError) as excinfo:
            message_received([50000], FakeMessage())
        assert excinfo.value.value['status_code'] == 500
        assert excinfo.value.value['uri'] == '/registration'
        assert excinfo.value.value['registration_no'] == 50000

    @mock.patch('requests.get', return_value=has_trading_resp)
    @mock.patch('requests.put', return_value=FakeResponse(status_code=500))
    def test_error_put_failed(self, mock_put, mock_get):
        with pytest.raises(SynchroniserError) as excinfo:
            message_received([50000], FakeMessage())
        assert excinfo.value.value['status_code'] == 500
        assert excinfo.value.value['uri'] == '/land_charge'
        assert excinfo.value.value['registration_no'] == 50000

    def test_app_root(self):
        response = self.app.get('/')
        assert response.status_code == 200

    @mock.patch('requests.get', return_value=three_errors_resp)
    def test_queues_error_count(self, mock):
        response = self.app.get('/queues/error')
        data = json.loads(response.data.decode('utf-8'))
        assert response.status_code == 200
        assert data['queue_length'] == 3

    @mock.patch('requests.get', return_value=FakeResponse(status_code=501))
    def test_queues_catch_mqadmin_failure(self, mock):
        response = self.app.get('/queues/error')
        data = json.loads(response.data.decode('utf-8'))
        assert response.status_code == 500
        assert data['api_status'] == 501
