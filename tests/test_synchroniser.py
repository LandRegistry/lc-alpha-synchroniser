import pytest
from unittest import mock
import requests
import os
from application.utility import encode_name


# Can't use data from production system (non-public data); can't tell which pre-prod data is copied from production.
# Data in lower test regions is of unknown reliability.
# Solution: create script in another language to implement the coding rules to create
# this test data. Script checks out OK on pre-prod examples.
test_names = [
    {
        "input": {"forenames": ["Adelle", "Shaylee"], "surname": "Renner"},
        "expected": {"coded_name": "RENNEREELYAH", "remainder_name": "ADELLES", "hex_code": "26C7"}
    }, {
        "input": {"forenames": ["Bob", "Oscar", "Francis"], "surname": "Howard"},
        "expected": {"coded_name": "DRAWOHSICNAR", "remainder_name": "BOBOSCARF", "hex_code": "2325C7"},
    }, {
        "input": {"forenames": ["Dominique"], "surname": "O'Brien"},
        "expected": {"coded_name": "NEIRBOEUQINI", "remainder_name": "DOM", "hex_code": "C961"}
    }
]


class TestSynchroniser:
    def setup_method(self, method):
        pass

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