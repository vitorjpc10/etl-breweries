import pytest
import os
import json


@pytest.fixture(scope='session')
def transformed_data():
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    extracted_data_path = os.path.join(base_path, 'data', 'transformed', 'raw', 'json', 'formatted_breweries_data.json')

    with open(extracted_data_path, 'r') as file:
        data = json.load(file)

    return data

def test_transformation_not_empty(transformed_data):
    assert len(transformed_data) > 0, "Transformed data is empty"

def test_transformation_no_none_values(transformed_data):
    for entry in transformed_data:
        for key, value in entry.items():
            assert value is not None, f"None value found for key {key} in entry {entry}"

def test_transformation_has_required_keys(transformed_data):
    required_keys = {'id', 'name', 'brewery_type', 'address_1', 'address_2', 'address_3', 'city', 'state_province', 'postal_code', 'country', 'longitude', 'latitude', 'phone', 'website_url', 'state', 'street'}
    for entry in transformed_data:
        assert required_keys.issubset(entry.keys()), f"Missing keys in entry: {entry}"

def test_transformation_all_values_are_strings(transformed_data):
    for entry in transformed_data:
        for key in entry.keys():
            if entry[key] is not None:
                assert isinstance(entry[key], str), f"Value for {key} is not a string in entry: {entry}"

def test_transformation_valid_url_format(transformed_data):
    import re
    url_pattern = re.compile(r'^(http|https)://')
    for entry in transformed_data:
        if entry['website_url']:
            assert url_pattern.match(entry['website_url']), f"Invalid URL format in entry: {entry}"


