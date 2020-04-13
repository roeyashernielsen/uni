import numpy as np
import pandas as pd

from src.uni.flow.uflow import UFlow
from src.uni.flow.ustep import UStep
from src.uni.utils.spark import spark
import databricks.koalas as ks


@UStep
def get_dataABC(**kwargs) -> pd.DataFrame:
    df = spark.createDataFrame([('Alice', 1)])
    df.createOrReplaceTempView("roey_table")
    df.show()
    data = pd.DataFrame(np.arange(12).reshape(3, 4), columns=['A', 'B', 'C', 'D'])
    return data


@UStep
def get_dataXYZ(**kwargs) -> pd.DataFrame:
    spark.sql("select * from roey_table").show()
    data = pd.DataFrame(np.arange(3).reshape(3, 1), columns=['X'])
    return data


@UStep
def clean_data(table: pd.DataFrame, **kwargs) -> pd.DataFrame:
    table = table.applymap(lambda x: np.nan if x % 3 == 0 else x)
    return table


@UStep
def generate_features(table1: pd.DataFrame, table2: pd.DataFrame, **kwargs) -> pd.DataFrame:
    features = pd.concat((table1, table2), axis=1)
    return features


@UStep
def train_model_RED(features: pd.DataFrame, **kwargs) -> np.array:
    return pd.DataFrame([1, 3, 5, 7])


@UStep
def train_model_ROEY(features: pd.DataFrame, **kwargs) -> np.array:
    return pd.DataFrame([2, 4, 6, 8])


@UStep
def export_model(model: np.array, path: str, **kwargs) -> None:
    model.to_csv(path, index=False)


# Doesn't work without specify the param
with UFlow('sample_flow') as flow:
    data = get_dataABC()
    target_variable = get_dataXYZ()
    data = clean_data(table=data)
    target_variable = clean_data(table=target_variable)
    features = generate_features(table1=data, table2=target_variable)
    model1 = train_model_RED(features=features)
    model2 = train_model_ROEY(features=features)
    export_model(model=model1, path='model1.csv')
    export_model(model=model2, path='model2.csv')

# flow.show()
flow.run()
