import mlflow
import numpy as np
import pandas as pd
import pytest

import pyspark.sql as ssql
from src.uni import io
from src.uni.io import reader, writer
from src.uni.utils.spark import get_spark_session


class TestIO:
    file_format = [io.PyObjFileFormat.Pickle, io.PyObjFileFormat.CloudPickle]

    # Todo import test with mlflow_logging
    @pytest.mark.parametrize("file_format", file_format)
    @pytest.mark.parametrize("mlflow_logging", [True, False])
    def test_py_obj_write_read(self, file_format, mlflow_logging):
        with mlflow.start_run():
            df = pd.get_dummies(pd.Series(list("abca")))
            path = writer.save_py_obj(
                obj=df,
                obj_name="test_df",
                file_format=file_format,
                mlflow_logging=mlflow_logging,
            )
            df2 = reader.load_py_obj(obj_path=path, file_format=file_format)
            assert df.equals(df2)

    file_format = [io.TabularFileFormats.Parquet, io.TabularFileFormats.Feather]

    # Todo import test with mlflow_logging
    @pytest.mark.parametrize("file_format", file_format)
    @pytest.mark.parametrize("mlflow_logging", [True, False])
    def test_pd_df_write_read(self, file_format, mlflow_logging):
        with mlflow.start_run():
            df = pd.get_dummies(pd.Series(list("abcd")))
            path = writer.save_pd_df(
                df=df,
                df_name="test_df",
                file_format=file_format,
                mlflow_logging=mlflow_logging,
            )
            df2 = reader.load_pd_df(df_path=path, file_format=file_format)
            assert df.equals(df2)

    # Todo import test with mlflow_logging
    @pytest.mark.parametrize("mlflow_logging", [True, False])
    def test_spark_df_write_read(self, mlflow_logging):
        with mlflow.start_run():
            pdf = pd.DataFrame(np.random.rand(100, 3))
            spark = get_spark_session()
            df = spark.createDataFrame(pdf)
            path = writer.save_spark_df(
                df=df, df_name="test_df", mlflow_logging=mlflow_logging
            )
            df2 = reader.load_spark_df(df_path=path,)
            assert df.subtract(df2).count() == 0

    spark = get_spark_session()
    py_list = [1, 2, 3, 4, 5]
    pdf = pd.DataFrame(np.random.rand(100, 3))
    pdf.columns = ["Col_1", "Col_2", "Col_3"]
    df = spark.createDataFrame(pdf)
    objects = [df, pdf, py_list]

    # Todo import test with mlflow_logging
    @pytest.mark.parametrize("obj", objects)
    @pytest.mark.parametrize("mlflow_logging", [True, False])
    def test_write_read(self, obj, mlflow_logging):
        with mlflow.start_run():
            path = writer.save(obj=obj, name="test_obj")
            if isinstance(obj, pd.DataFrame):
                obj2 = reader.load(path=path, obj_type=io.ObjType.PandasDF.value)
                assert obj.equals(obj2)
            elif isinstance(obj, ssql.DataFrame):
                obj2 = reader.load(path=path, obj_type=io.ObjType.SparkDF.value)
                assert obj.subtract(obj2).count() == 0
            else:
                obj2 = reader.load(path=path)
                assert obj == obj2
