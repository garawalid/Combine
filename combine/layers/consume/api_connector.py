import json
import requests
from .connector import Connector


class ApiConnector(Connector):

    def __init__(self, url, api_key):
        super(ApiConnector, self).__init__()
        self.url = url
        self.api_key = api_key

    def download_data(self, api_url):

        headers = {'user-agent': 'wgara'}
        response = requests.get(api_url, headers=headers)

        if response.status_code == 200:
            return json.loads(response.content.decode('utf-8'))
        else:
            # Todo : raise an error
            return None

    def url_builder(self):
        # This class is specific to each API.
        # It takes the url and api key and return a ready url to consume data
        raise NotImplementedError('url_builder() is not implemented !')
