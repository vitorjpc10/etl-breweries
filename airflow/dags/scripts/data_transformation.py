import pandas as pd

class Transform:

    def format_brewery_data(self, data):
        """
        Formats brewery data dictionaries.

        Args:
        data (list of dict): List of dictionaries containing brewery data.

        Returns:
        list of dict: Formatted list of brewery data dictionaries.
        """

        for entry in data:
            entry['city'] = entry['city'].title() if entry.get('city') else ''
            entry['state'] = entry['state'].title() if entry.get('state') else ''
            entry['state_province'] = entry['state_province'].title() if entry.get('state_province') else ''
            entry['country'] = entry['country'].title() if entry.get('country') else ''

            for key, value in entry.items():
                entry[key] = value if value else ''

        return data

    def create_aggregated_view_brewery_type(self, data, location_keys: list):
        """
        Creates an aggregated view with the quantity of breweries per type and location.

        Args:
        data (list of dict): Cleaned list of brewery data dictionaries.
        location_keys (list of str): List of keys to group by (e.g., ['state', 'city']).

        Returns:
        list of dict: Aggregated list of dictionaries.

        Raises:
        ValueError: If any of the location keys are not found in the DataFrame columns.
        """
        df = pd.DataFrame(data)

        # Check if all location keys are present in the DataFrame columns
        missing_keys = [key for key in location_keys if key not in df.columns]
        if missing_keys:
            raise ValueError(f"The following keys are not found in the DataFrame columns: {missing_keys}")

        # Ensure that 'brewery_type' is included in the grouping keys
        group_by_keys = location_keys.copy()
        group_by_keys.append('brewery_type')

        agg_df = df.groupby(group_by_keys).size().reset_index(name='count')

        # Convert DataFrame back to list of dictionaries
        agg_list = agg_df.to_dict(orient='records')

        return agg_list