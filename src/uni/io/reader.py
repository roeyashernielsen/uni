"""Reader utils."""
import pickle
import os

from urllib.parse import unquote, urlparse

import cloudpickle
import pandas as pd

from ..io import PyObjFileFormat, TabularFileFormats
from ..utils.spark import spark


def load_py_obj(file_path,
                file_name=None,
                file_format=PyObjFileFormat.Pickle,
                mlflow_artifact=True):
    """
    Loads object.

    Loads data from a data source
    and returns it as a :class:`DataFrame`.
    If name is not None, load the df from the MLflow artifact
    """
    if file_name:
        file_path = os.path.join(file_path, file_name)
    if mlflow_artifact:
        file_path = unquote(urlparse(file_path).path)

    with open(file_path, "rb") as file:
        if file_path.endswith(PyObjFileFormat.Pickle.value) \
                or file_format == PyObjFileFormat.Pickle:
            return pickle.load(file)
        elif file_path.endswith(PyObjFileFormat.CloudPickle.value) \
                or file_format == PyObjFileFormat.CloudPickle:
            return cloudpickle.load(file)
        else:
            raise ValueError(f"file_format must be a PyObjFileFormat but got {type(file_format)}")


def load_pd_df(df_path,
               df_name=None,
               columns=None,
               file_format=TabularFileFormats.Parquet,
               mlflow_artifact=True,
               **kwargs):
    """
    Loads object.

    Loads data from a data source
    and returns it as a :class:`DataFrame`.
    If name is not None, load the df from the MLflow artifact
    """
    if df_name:
        df_path = os.path.join(df_path, df_name)
    if mlflow_artifact:
        df_path = unquote(urlparse(df_path).path)

    if df_path.endswith(TabularFileFormats.Parquet.value) \
            or file_format == TabularFileFormats.Parquet:
        return pd.read_parquet(path=df_path, engine="pyarrow", columns=columns, **kwargs)
    elif df_path.endswith(TabularFileFormats.Feather.value) \
            or file_format == TabularFileFormats.Feather:
        return pd.read_feather(path=df_path, columns=columns, use_threads=True)
    else:
        raise ValueError(f"file_format must be a PyObjFileFormat but got {type(file_format)}")


def load_spark_df(df_path,
                  df_name=None,
                  columns=None,
                  file_format=TabularFileFormats.Parquet,
                  mlflow_artifact=True,
                  **kwargs):
    """
    Loads object.

    Loads data from a data source
    and returns it as a :class:`DataFrame`.
    If name is not None, load the df from the MLflow artifact
    """
    if df_name:
        df_path = os.path.join(df_path, df_name)
    if mlflow_artifact:
        df_path = unquote(urlparse(df_path).path)

    if file_format == TabularFileFormats.Parquet:
        return spark.read.parquet(df_path)
    else:
        raise ValueError(f"file_format must be a PyObjFileFormat but got {type(file_format)}")
