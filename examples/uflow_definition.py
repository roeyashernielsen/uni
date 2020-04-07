import random

import numpy as np
import pandas as pd

import pyspark.sql as ssql
from src.uni.flow.uflow import UFlow
from src.uni.flow.ustep import UStep
from src.uni.utils.spark import get_spark_session


@UStep
def get_rand(**kwargs) -> int:
    rand = random.randint(0, 100)
    return rand


@UStep
def get_dataABC(rand, **kwargs) -> pd.DataFrame:
    data = pd.DataFrame(np.arange(12).reshape(3, 4), columns=["A", "B", "C", "D"])
    return data


@UStep
def get_dataXYZ(rand, **kwargs) -> pd.DataFrame:
    data = pd.DataFrame(np.arange(3).reshape(3, 1), columns=["X"])
    return data


@UStep
def clean_data(table: pd.DataFrame, **kwargs) -> pd.DataFrame:
    table = table.applymap(lambda x: np.nan if x % 3 == 0 else x)
    return table


@UStep
def generate_features(
    table1: pd.DataFrame, table2: pd.DataFrame, **kwargs
) -> pd.DataFrame:
    features = pd.concat((table1, table2), axis=1)
    return features


@UStep
def train_model_RED(features: pd.DataFrame, **kwargs) -> np.array:
    spark = get_spark_session()
    df = spark.createDataFrame(features)
    return df


@UStep
def train_model_ROEY(features: pd.DataFrame, **kwargs) -> np.array:
    spark = get_spark_session()
    df = spark.createDataFrame(features)
    return df


@UStep
def export_model(model: ssql.DataFrame, path: str, **kwargs) -> None:
    model.write.csv(path)


# Doesn't work without specify the param
with UFlow("example_flow") as flow:
    rand = get_rand.step()
    data = get_dataABC.step(rand=rand)
    target_variable = get_dataXYZ.step(rand=rand)
    data = clean_data.step(table=data)
    target_variable = clean_data.step(table=target_variable)
    features = generate_features.step(table1=data, table2=target_variable)
    model1 = train_model_RED.step(features=features)
    model2 = train_model_ROEY.step(features=features)
    export_model.step(model=model1, path="model1.csv")
    export_model.step(model=model2, path="model2.csv")
