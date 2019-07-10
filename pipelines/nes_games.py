import os
import sys

current_path = os.path.abspath('.')
parent_path = os.path.dirname(current_path)
sys.path.append(parent_path)

import json
import logging
from datetime import datetime

from combine.layers.batch.utils_inmemory import read_table, purge_table, rename_dataframe_json
from combine.layers.consume.api_connector import ApiConnector
from combine.pipelines.pipeline import Pipeline

logging.basicConfig(format='%(asctime)s %(message)s')

import pandas as pd

pd.set_option('display.max_columns', 20)


class NesConnector(ApiConnector):
    def __init__(self, url, api_key, table_name):
        super(NesConnector, self).__init__(url, api_key)
        # useful to track the sync
        self.offset = 0  # Todo: private attribute
        self.limit = None
        self.number_of_page_results = None
        self.number_of_total_results = None

        self.configuration = self.get_configuration()

        self._table_name = table_name  # table name in the datalake
        self._timestamp = int(datetime.now().timestamp())

    def url_builder(self, offset=None, platforms=None, date_last_updated=None):
        url = "{}?api_key={}&format=json".format(self.url, self.api_key)

        if not isinstance(offset, int) and offset is not None:
            raise ValueError("offset should be an integer.")

        if not isinstance(platforms, int) and platforms is not None:
            raise ValueError('platforms should be an integer.')

        if not isinstance(date_last_updated, str) and date_last_updated is not None:
            raise ValueError('date_last_updated should be a string.')

        if offset is not None:
            url = "{}&offset={}".format(url, offset)

        if platforms is not None:
            url = "{}&platforms={}".format(url, platforms)

        if date_last_updated is not None:
            date_now = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
            url = "{}&filter=date_last_updated:{}|{}".format(url, date_last_updated, date_now)

        print('url : {}'.format(url))

        return url

    def fetch_data(self, offset=None, date_last_updated=None, platforms=None):
        api_url = self.url_builder(offset=offset, date_last_updated=date_last_updated, platforms=platforms)
        data = self.download_data(api_url)
        self.update_metadata(data)
        # print('data : {}'.format(data))
        # fixme: disable this method, it's for debug purpose
        self.print_metadata()

        self.put_data(data)

    def sync(self):
        # Download new data starting from date_last_sync
        date_last_sync = self.configuration['date_last_sync']

        # purge table
        purge_table(self._table_name)

        self.fetch_data(date_last_updated=date_last_sync, platforms=self.configuration['platforms'])

        # Iterate throw pages if we have multiple pages :
        while (self._remaining_page()):
            # update offset
            self.offset = self.offset + self.number_of_page_results

            self.fetch_data(date_last_updated=date_last_sync, offset=self.offset)

    def _remaining_page(self):
        # check if still some pages to iterate in this query .
        return self.number_of_total_results > self.offset

    def get_configuration(self):
        # Get configration from MongoDB.
        # For samplicity, we will make it constant
        return {'date_last_sync': '2019-06-05 16:54:07', 'platforms': 21}  # Todo: add credentials support

    def update_metadata(self, response):
        self.limit = response['limit']
        self.offset = response['offset']
        self.number_of_page_results = response['number_of_page_results']
        self.number_of_total_results = response['number_of_total_results']

    def print_metadata(self):
        print('limit : {}'.format(self.limit))
        print('offset : {}'.format(self.offset))
        print('number_of_page_results : {}'.format(self.number_of_page_results))
        print('number_of_total_results : {}'.format(self.number_of_total_results))

    def put_data(self, data=None):
        # save each query as file in the datalake
        if data is None:
            raise ValueError('data is empty !')

        file = "../datalake/{}/{}_{}.json".format(self._table_name, self.offset, self._timestamp)
        print('saving file {} in {}'.format(file, self._table_name))  # Todo : change it to logging message
        with open(file, 'w') as f:
            json.dump(data, f)


class NesGames(Pipeline):
    def __init__(self, id=None, name=None, data_source=None, data_destination=None, responsible=None, url=None,
                 api_key=None, table_name=None):
        super(NesGames, self).__init__(id, name, data_source, data_destination, responsible, table_name=table_name)

        if url is None or api_key is None:
            raise ValueError('We cannot start a pipeline without url and api_key parameters.')

        self.url = url
        self.api_key = api_key
        self._timestamp = int(datetime.now().timestamp())

    def run(self):
        self._load_data()
        self._preprocess()
        self._process()
        self._postprocess()
        self._save()

    def _load_data(self):
        # load data from API and save it in the datalake
        connector = NesConnector(url=self.url, api_key=self.api_key, table_name=self.table_name)
        connector.sync()

    def _preprocess(self):
        self.data = read_table("nes", type="json")

        print(self.data.head())
        print('count df : {}'.format(self.data.count()[0]))

    def _process(self):
        self.data = self._normalize_dataframe(self.data)
        self.data = rename_dataframe_json(self.data)
        # Drop null names
        self.data = self.data.dropna(subset=['name'])

    def _postprocess(self):
        # Todo: Compute some stats about the final output
        # Todo : Compute the time of execution of the pipeline
        pass

    def _save(self):
        # Update date_last_sync
        # Todo : we didn't implement this part yet ! We need to mount a database in order to
        #  keep tracking this value.
        print('The final dataframe:')
        print(self.data.head(10))
        self.data.to_csv("../datawarehouse/res/output_{}.csv".format(self._timestamp))

    def _normalize_dataframe(self, df):
        """
        Normalize the first level of a DataFrame that contains json data.

        Parameters
        ----------
        df : Pandas DataFrame
            The DataFrame to normalize

        Returns
        -------
        Pandas DataFrame

        """
        json_struct = json.loads(df[['results']].to_json(orient="records"))
        df_flat = pd.io.json.json_normalize(json_struct)

        return df_flat


if __name__ == "__main__":
    nes_games = NesGames(url="https://www.giantbomb.com/api/games/", api_key="6fe6fb576b0c7bef2364938b2248e1628759508d",
                         table_name="nes")
    nes_games.run()
