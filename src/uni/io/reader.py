"""Reader utils."""
import pickle
from urllib.parse import urlparse

import cloudpickle
import pandas as pd

from ..io import ObjType, PyObjFileFormat, TabularFileFormats
from ..utils.spark import get_spark_session


def load(path, obj_type=None):
    if obj_type == ObjType.PandasDF.value:
        return load_pd_df(df_path=path)
    elif obj_type == ObjType.SparkDF.value:
        return load_spark_df(df_path=path)
    else:
        return load_py_obj(obj_path=path)


def load_py_obj(obj_path, file_format=PyObjFileFormat.Pickle):
    """
    Loads object.

    Loads data from a data source
    and returns it as a :class:`DataFrame`.
    If name is not None, load the df from the MLflow artifact
    """

    with open(urlparse(obj_path).path, "rb") as file:
        if (
            obj_path.endswith(PyObjFileFormat.Pickle.value)
            or file_format == PyObjFileFormat.Pickle
        ):
            return pickle.load(file)
        elif (
            obj_path.endswith(PyObjFileFormat.CloudPickle.value)
            or file_format == PyObjFileFormat.CloudPickle
        ):
            return cloudpickle.load(file)
        else:
            raise ValueError(
                f"file_format must be a PyObjFileFormat but got {type(file_format)}"
            )


def load_pd_df(df_path, columns=None, file_format=TabularFileFormats.Parquet, **kwargs):
    """
    Loads object.

    Loads data from a data source
    and returns it as a :class:`DataFrame`.
    If name is not None, load the df from the MLflow artifact
    """

    if (
        df_path.endswith(TabularFileFormats.Parquet.value)
        or file_format == TabularFileFormats.Parquet
    ):
        return pd.read_parquet(df_path, engine="pyarrow", columns=columns, **kwargs)
    elif (
        df_path.endswith(TabularFileFormats.Feather.value)
        or file_format == TabularFileFormats.Feather
    ):
        return pd.read_feather(path=df_path, columns=columns, use_threads=True)
    else:
        raise ValueError(
            f"file_format must be a PyObjFileFormat but got {type(file_format)}"
        )


def load_spark_df(df_path, file_format=TabularFileFormats.Parquet):
    """
    Loads object.

    Loads data from a data source
    and returns it as a :class:`DataFrame`.
    If name is not None, load the df from the MLflow artifact
    """

    if file_format == TabularFileFormats.Parquet:
        return spark.read.parquet(df_path)
    else:
        raise ValueError(
            f"file_format must be a PyObjFileFormat but got {type(file_format)}"
        )
