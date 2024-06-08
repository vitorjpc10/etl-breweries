import requests

class Extract:

    def __init__(self):
        self.brewery_url = 'https://api.openbrewerydb.org/breweries'

    def get_breweries(self):
        response = requests.get(self.brewery_url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Error Occurred calling brewery API.\n"
                            f"Error: {response.status_code}, 'message': {response.reason}")

