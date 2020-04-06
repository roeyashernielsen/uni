import pytest
import pandas as pd
import numpy as np

from src.uni import io
from src.uni.io import writer, reader
from src.uni.utils.spark import spark


class TestIO:
    file_format = [io.PyObjFileFormat.Pickle, io.PyObjFileFormat.CloudPickle]

    # Todo import test with mlflow_logging
    @pytest.mark.parametrize("file_format", file_format)
    @pytest.mark.parametrize("mlflow_logging", [True, False])
    def test_py_obj_write_read(self, file_format, mlflow_logging):
        df = pd.get_dummies(pd.Series(list('abca')))
        path = writer.save_py_obj(
            obj=df,
            name="test_df",
            file_format=file_format,
            mlflow_logging=mlflow_logging)
        df2 = reader.load_py_obj(
            file_path=path,
            file_format=file_format,
            mlflow_artifact=mlflow_logging)
        assert (df.equals(df2))

    file_format = [io.TabularFileFormats.Parquet, io.TabularFileFormats.Feather]

    # Todo import test with mlflow_logging
    @pytest.mark.parametrize("file_format", file_format)
    @pytest.mark.parametrize("mlflow_logging", [True, False])
    def test_pd_df_write_read(self, file_format, mlflow_logging):
        df = pd.get_dummies(pd.Series(list('abcd')))
        path = writer.save_pd_df(
            df=df,
            df_name="test_df",
            file_format=file_format,
            mlflow_logging=mlflow_logging)
        df2 = reader.load_pd_df(
            df_path=path,
            file_format=file_format,
            mlflow_artifact=mlflow_logging)
        assert (df.equals(df2))

    file_format = [io.TabularFileFormats.Parquet]

    # Todo import test with mlflow_logging
    @pytest.mark.parametrize("file_format", file_format)
    @pytest.mark.parametrize("mlflow_logging", [True, False])
    def test_spark_df_write_read(self, file_format, mlflow_logging):
        pdf = pd.DataFrame(np.random.rand(100, 3))
        df = spark.createDataFrame(pdf)
        path = writer.save_spark_df(
            df=df,
            df_name="test_df",
            file_format=file_format,
            mlflow_logging=mlflow_logging)
        df2 = reader.load_spark_df(
            df_path=path,
            file_format=file_format,
            mlflow_artifact=mlflow_logging)
        assert (df.subtract(df2).count() == 0)
