import logging
import re

class ExtractionDataTests:
    def __init__(self, data):
        self.transformed_data = data

    def run_tests(self):
        """
        Runs all data tests.

        Raises:
        Exception: If any test fails.
        """

        self.extraction_not_empty()
        self.extraction_has_required_keys()
        self.all_values_are_strings()
        self.valid_url_format()
        logging.info("All Extraction data tests passed successfully.")

    def extraction_not_empty(self):
        """
       Checks if the extracted data is not empty.

       Raises:
       Exception: If the extracted data is empty.
       """

        if not len(self.transformed_data) > 0:
            raise Exception("Extracted data is empty")

    def extraction_has_required_keys(self):
        """
        Checks if each entry in the extracted data has all required keys.

        Raises:
        Exception: If any entry is missing required keys.
        """

        required_keys = {'id', 'name', 'brewery_type', 'address_1', 'address_2', 'address_3', 'city', 'state_province', 'postal_code', 'country', 'longitude', 'latitude', 'phone', 'website_url', 'state', 'street'}
        for entry in self.transformed_data:
            if not required_keys.issubset(entry.keys()):
                raise Exception(f"Missing keys in entry: {entry}")

    def all_values_are_strings(self):
        """
       Checks if all values in the extracted data are strings.

       Raises:
       Exception: If any value is not a string.
       """

        for entry in self.transformed_data:
            for key in entry.keys():
                if entry[key] is not None and not isinstance(entry[key], str):
                    raise Exception(f"Value for {key} is not a string in entry: {entry}")

    def valid_url_format(self):
        """
        Checks if the 'website_url' in each entry of the extracted data has a valid URL format.

        Raises:
        Exception: If any 'website_url' has an invalid format.
        """

        url_pattern = re.compile(r'^(http|https)://')
        for entry in self.transformed_data:
            if entry['website_url'] and not url_pattern.match(entry['website_url']):
                raise Exception(f"Invalid URL format in entry: {entry}")



