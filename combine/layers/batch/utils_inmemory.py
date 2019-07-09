# Set of in-Memory methods

import pandas as pd


def read_data(table_name="", type=None):
    if not isinstance(table_name, str):
        raise ValueError("table_name should be a string.")

    if len(table_name) == 0:
        raise ValueError("Please pass a valid table_name.")
    else:
        path = "../datalake/{}".format(table_name)
    if type is None:
        raise ValueError('Invalid type')

    if type.lower() == "csv":
        raise NotImplementedError('CSV type is not implemented yet!')
    if type.lower() == "json":
        print("path : {}".format(path))
        df = pd.read_json(path)
    else:
        raise NotImplementedError('Type = {} is not implemnted yet'.format(type))

    return df


def save_data(table_name='', type=None):
    raise NotImplementedError('save_data is not implemented yet!')
