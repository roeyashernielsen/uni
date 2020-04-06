"""Writers utils."""
import os
import pickle
import tempfile

import mlflow
import pandas as pd
import cloudpickle
import pyspark.sql as ssql

from ..io import PyObjFileFormat, TabularFileFormats


def _get_file_path(file_name, dir_path):
    if dir_path is None:
        return os.path.join(tempfile.mkdtemp(), file_name)
    else:
        return os.path.join(dir_path, file_name)


def save_py_obj(
        obj,
        name,
        dir_path=None,
        file_format=PyObjFileFormat.Pickle,
        mlflow_logging=True
):
    """
    Saves the contents of the obj to a pickle file.

    In addition, log the file or directory as an artifact
    of the currently MLflow active run.
    """
    file_name = name + file_format.value
    file_path = _get_file_path(file_name, dir_path)

    with open(file_path, "wb") as file:
        if file_format == PyObjFileFormat.Pickle:
            pickle.dump(obj, file)
        elif file_format == PyObjFileFormat.CloudPickle:
            cloudpickle.dump(obj, file)
        else:
            raise ValueError(f"file_format must be a PyObjFileFormat but got {type(file_format)}")

    if mlflow_logging:
        mlflow.log_artifact(file_path, name)
        mlflow.log_param(name, mlflow.get_artifact_uri(name))

    return file_path


def save_pd_df(
        df,
        df_name,
        dir_path=None,
        file_format=TabularFileFormats.Parquet,
        partition_by=None,
        mlflow_logging=True,
        **kwargs
):
    """
    Saves the contents of the :class:`DataFrame` to a data source.

    In addition, log the file or directory as an artifact
    of the currently MLflow active run.
    If no run is active, this method will create a new active run.
    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError(f"df must be a Pandas DataFrame but got {type(df)}")

    file_name = df_name + file_format.value
    file_path = _get_file_path(file_name, dir_path)

    if file_format == TabularFileFormats.Parquet:
        df.to_parquet(path=file_path, engine="pyarrow", partition_cols=partition_by, **kwargs)
    elif file_format == TabularFileFormats.Feather:
        df.to_feather(path=file_path, **kwargs)
    else:
        raise ValueError(f"file_format must be a TabularFileFormats but got {type(file_format)}")

    if mlflow_logging:
        mlflow.log_artifact(file_path, df_name)
        mlflow.log_param(df_name, mlflow.get_artifact_uri(df_name))

    return file_path


def save_spark_df(
        df,
        df_name,
        dir_path=None,
        partition_by=None,
        file_format=TabularFileFormats.Parquet,
        mlflow_logging=True,
        **kwargs
):
    """
    Saves the contents of the :class:`DataFrame` to a data source.

    In addition, log the file or directory as an artifact
    of the currently MLflow active run.
    If no run is active, this method will create a new active run.
    """
    if not isinstance(df, ssql.DataFrame):
        raise ValueError(f"df must be a Spark DataFrame but got {type(df)}")

    path = _get_file_path(df_name, dir_path)

    if file_format == TabularFileFormats.Parquet:
        df.write.parquet(path=path, mode="overwrite", partitionBy=partition_by, compression="snappy")
    else:
        raise ValueError(f"file_format must be a TabularFileFormats but got {type(file_format)}")

    if mlflow_logging:
        mlflow.log_artifacts(path, df_name)
        mlflow.log_param(df_name, mlflow.get_artifact_uri(df_name))

    return path
