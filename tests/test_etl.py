import os
import sys
import pytest

# Add the scripts directory to the sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))

from scripts.main import main

@pytest.fixture
def mock_dependencies(mocker):
    mock_extract = mocker.patch('main.Extract')
    mock_transform = mocker.patch('main.Transform')
    mock_loading = mocker.patch('main.Loading')

    # Mock the Extract methods
    mock_extract_instance = mock_extract.return_value
    mock_extract_instance.get_weather.return_value = {"weather": "data"}
    mock_extract_instance.get_traffic.return_value = {"traffic": "data"}

    # Mock the Transform methods
    mock_transform_instance = mock_transform.return_value
    mock_transform_instance.clean_weather_data.return_value = {"weather": "transformed"}
    mock_transform_instance.clean_traffic_data.return_value = {"traffic": "transformed"}

    # Mock the Loading methods
    mock_loading_instance = mock_loading.return_value
    mock_loading_instance.load_data.return_value = None

    return mock_extract_instance, mock_transform_instance, mock_loading_instance

def test_etl_process(mock_dependencies):
    mock_extract_instance, mock_transform_instance, mock_loading_instance = mock_dependencies

    # Run the ETL process
    main()

    # Check that the extract methods were called
    mock_extract_instance.get_weather.assert_called_once()
    mock_extract_instance.get_traffic.assert_called_once()

    # Check that the transform methods were called
    mock_transform_instance.clean_weather_data.assert_called_once_with({"weather": "data"})
    mock_transform_instance.clean_traffic_data.assert_called_once_with({"traffic": "data"})

    # Check that the load methods were called
    mock_loading_instance.load_data.assert_any_call({"weather": "transformed"}, "weather")
    mock_loading_instance.load_data.assert_any_call({"traffic": "transformed"}, "traffic")

def teardown_module(module):
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    extracted_path = os.path.join(base_path, 'data', 'extracted')
    transformed_path = os.path.join(base_path, 'data', 'transformed')
    weather_file = os.path.join(extracted_path, 'extracted_weather_data.json')
    traffic_file = os.path.join(extracted_path, 'extracted_traffic_data.json')
    transformed_weather_file = os.path.join(transformed_path, 'transformed_weather_data.json')
    transformed_traffic_file = os.path.join(transformed_path, 'transformed_traffic_data.json')

    for file in [weather_file, traffic_file, transformed_weather_file, transformed_traffic_file]:
        if os.path.exists(file):
            os.remove(file)
