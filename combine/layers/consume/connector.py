class Connector:
    def __init__(self):
        pass

    def download_data(self):
        raise NotImplementedError('download_data is not implemented.')

    def get_configuration(self):
        raise NotImplementedError('get_configuration is not implemented yet!')

    def put_data(self, datalake_table=None):
        # put data in datalake
        raise NotImplementedError('put_data is not implemented yet!')
