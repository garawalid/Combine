import unittest
from combine.layers.consume.api_connector import ApiConnector


class ApiConnectorTest(unittest.TestCase):

    @property
    def connector(self):
        return ApiConnector(url="https://www.giantbomb.com/api/games/",
                            api_key="6fe6fb576b0c7bef2364938b2248e1628759508d")

    def test_download_data(self):
        result = self.connector.download_data(
            api_url="https://www.giantbomb.com/api/games/?api_key=6fe6fb576b0c7bef2364938b2248e1628759508d&format=json&field_list=id")
        assert (result['error'] == "OK")

        result = self.connector.download_data(api_url="https://www.giantbomb.com/api/games/?api_key=6fe6f")
        assert (result is None)
