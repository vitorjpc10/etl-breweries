import os
import sys
import json
import pytest
# Add the scripts directory to the sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))


from scripts.main import save_to_json_file

@pytest.fixture
def setup_files():
    test_data = {"key": "value"}
    test_file = "test.json"
    yield test_data, test_file
    if os.path.exists(test_file):
        os.remove(test_file)

def test_write_dict_to_file_success(setup_files):
    test_data, test_file = setup_files
    save_to_json_file(test_data, test_file)
    assert os.path.exists(test_file)

    with open(test_file, 'r') as file:
        data = json.load(file)
        assert data == test_data

def test_write_dict_to_file_failure():
    test_data = {"key": "value"}
    with pytest.raises(Exception):
        save_to_json_file(test_data, "/invalid/path/")
