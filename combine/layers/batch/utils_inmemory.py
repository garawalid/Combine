# Set of in-Memory methods

import pandas as pd
import subprocess
import glob
import os


def read_table(table_name="", type=None):
    if not isinstance(table_name, str):
        raise ValueError("table_name should be a string.")

    if len(table_name) == 0:
        raise ValueError("Please pass a valid table_name.")
    else:
        path = "../datalake/{}/".format(table_name)
    if type is None:
        raise ValueError('Invalid type')

    if type.lower() == "csv":
        raise NotImplementedError('CSV type is not implemented yet!')
    if type.lower() == "json":
        print("path : {}".format(path))

        df = _read_json_directory(path)
    else:
        raise NotImplementedError('Type = {} is not implemnted yet'.format(type))

    return df


def _read_json_directory(path):
    all_files = glob.glob(os.path.join(path, "*.json"))
    df_from_each_file = (pd.read_json(f) for f in all_files)
    concatenated_df = pd.concat(df_from_each_file, ignore_index=True)

    return concatenated_df


def save_data(table_name='', type=None):
    raise NotImplementedError('save_data is not implemented yet!')


def purge_table(table_name=None):
    if isinstance(table_name, str):
        print('Deleteing old table {} in datalake.'.format(table_name))
        # Todo : [Bug] Delete only files instead of the folder
        subprocess.Popen(['rm', '-rf', '../datalake/{}/'.format(table_name)])
        subprocess.Popen(['mkdir', '../datalake/{}'.format(table_name)])
    else:
        raise ValueError('Table_name should be a string.')


def rename_dataframe_json(df):
    # If the dataframe contains name of columns with dot. We just keep the last word in the name.
    # This is useful when we deal with Json file and after we normalize the data.
    df_new = df.copy()
    df_new.columns = [c.split('.')[-1] for c in df.columns]
    return df_new
