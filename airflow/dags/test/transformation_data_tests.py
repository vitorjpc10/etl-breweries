import re
import logging


class TransformationDataTests:
    def __init__(self, data):
        self.transformed_data = data

    def run_tests(self):
        """
        Runs all data tests.

        Raises:
        Exception: If any test fails.
        """

        self.transformation_not_empty()
        self.transformation_no_none_values()
        self.transformation_has_required_keys()
        self.transformation_all_values_are_strings()
        self.transformation_valid_url_format()
        logging.info("All transformation data tests passed successfully.")

    def transformation_not_empty(self):
        """
       Checks if the transformed data is not empty.

       Raises:
       Exception: If the transformed data is empty.
       """

        if not len(self.transformed_data) > 0:
            raise Exception("Transformed data is empty")

    def transformation_no_none_values(self):
        """
       Checks if there are no None values in the transformed data.

       Raises:
       Exception: If any None value is found.
       """

        for entry in self.transformed_data:
            for key, value in entry.items():
                if value is None:
                    raise Exception(f"None value found for key {key} in entry {entry}")

    def transformation_has_required_keys(self):
        """
       Checks if each entry in the transformed data has all required keys.

       Raises:
       Exception: If any entry is missing required keys.
       """

        required_keys = {'id', 'name', 'brewery_type', 'address_1', 'address_2', 'address_3', 'city', 'state_province',
                         'postal_code', 'country', 'longitude', 'latitude', 'phone', 'website_url', 'state', 'street'}
        for entry in self.transformed_data:
            if not required_keys.issubset(entry.keys()):
                raise Exception(f"Missing keys in entry: {entry}")

    def transformation_all_values_are_strings(self):
        """
       Checks if all values in the transformed data are strings.

       Raises:
       Exception: If any value is not a string.
       """

        for entry in self.transformed_data:
            for key in entry.keys():
                if entry[key] is not None and not isinstance(entry[key], str):
                    raise Exception(f"Value for {key} is not a string in entry: {entry}")

    def transformation_valid_url_format(self):
        """
        Checks if the 'website_url' in each entry of the transformed data has a valid URL format.

        Raises:
        Exception: If any 'website_url' has an invalid format.
        """

        url_pattern = re.compile(r'^(http|https)://')
        for entry in self.transformed_data:
            if entry['website_url'] and not url_pattern.match(entry['website_url']):
                raise Exception(f"Invalid URL format in entry: {entry}")
