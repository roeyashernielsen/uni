"""Writers utils."""
import os
import pickle
import tempfile

import pandas as pd
import cloudpickle
import pyspark.sql as ssql

from ..io import PyObjFileFormat, TabularFileFormats, ObjType, PathType
from ..utils.mlflow import ArtifactMD, log_artifact


def _get_file_path(name, extension=None, dir_path=None):
    if dir_path is None:
        dir_path = tempfile.mkdtemp()
    if extension is not None:
        name = name + extension
    return os.path.join(dir_path, name)


def save(obj, name, dir_path=None, mlflow_logging=True):
    """Save obj."""
    if isinstance(obj, pd.DataFrame):
        return save_pd_df(df=obj, df_name=name, dir_path=dir_path, mlflow_logging=mlflow_logging)
    elif isinstance(obj, ssql.DataFrame):
        return save_spark_df(df=obj, df_name=name, dir_path=dir_path, mlflow_logging=mlflow_logging)
    else:
        return save_py_obj(obj=obj, obj_name=name, dir_path=dir_path, mlflow_logging=mlflow_logging)


def save_py_obj(
        obj,
        obj_name,
        dir_path=None,
        file_format=PyObjFileFormat.Pickle,
        mlflow_logging=True
):
    """
    Saves the contents of the obj to a pickle file.

    In addition, log the file or directory as an artifact
    of the currently MLflow active run.
    """
    path = _get_file_path(name=obj_name, extension=file_format.value, dir_path=dir_path)

    with open(path, "wb") as file:
        if file_format == PyObjFileFormat.Pickle:
            pickle.dump(obj, file)
        elif file_format == PyObjFileFormat.CloudPickle:
            cloudpickle.dump(obj, file)
        else:
            raise ValueError(f"file_format must be a PyObjFileFormat but got {type(file_format)}")

    if mlflow_logging:
        log_artifact(
            ArtifactMD(
                name=obj_name,
                path=path,
                path_type=PathType.File,
                obj_type=ObjType.PyObj.value,
                file_format=file_format.value
            )
        )

    return path


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

    path = _get_file_path(name=df_name, extension=file_format.value, dir_path=dir_path)

    if file_format == TabularFileFormats.Parquet:
        df.to_parquet(path=path, engine="pyarrow", partition_cols=partition_by, **kwargs)
    elif file_format == TabularFileFormats.Feather:
        df.to_feather(path=path, **kwargs)
    else:
        raise ValueError(f"file_format must be a TabularFileFormats but got {type(file_format)}")

    if mlflow_logging:
        log_artifact(
            ArtifactMD(
                name=df_name,
                path=path,
                path_type=PathType.File,
                obj_type=ObjType.PandasDF.value,
                file_format=file_format.value
            )
        )

    return path


def save_spark_df(
        df,
        df_name,
        dir_path=None,
        partition_by=None,
        file_format=TabularFileFormats.Parquet,
        mlflow_logging=True,
):
    """
    Saves the contents of the :class:`DataFrame` to a data source.

    In addition, log the file or directory as an artifact
    of the currently MLflow active run.
    If no run is active, this method will create a new active run.
    """
    if not isinstance(df, ssql.DataFrame):
        raise ValueError(f"df must be a Spark DataFrame but got {type(df)}")

    path = _get_file_path(name=df_name, dir_path=dir_path)

    if file_format == TabularFileFormats.Parquet:
        df.write.parquet(path=path, mode="overwrite", partitionBy=partition_by, compression="snappy")
        # TODO add support with stempView
        # df.createOrReplaceTempView(df_name)
    else:
        raise ValueError(f"file_format must be a TabularFileFormats but got {type(file_format)}")

    if mlflow_logging:
        log_artifact(
            ArtifactMD(
                name=df_name,
                path=path,
                path_type=PathType.Directory,
                obj_type=ObjType.SparkDF.value,
                file_format=file_format.value
            )
        )

    return path
