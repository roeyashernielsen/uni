import random

import numpy as np
import pandas as pd
import pyspark.sql as ssql
from uni.flow import UStepType
from uni.flow.uflow import UFlow
from uni.flow.ustep import UStep


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


@UStep(step_type=UStepType.Spark)
def train_model_RED(features: pd.DataFrame, **kwargs) -> np.array:
    print(features)
    df = spark.createDataFrame([[1, 2, 3]], ["col0", "col1", "col2"])
    df.show()
    return df


@UStep(step_type=UStepType.Spark)
def train_model_ROEY(features: pd.DataFrame, **kwargs) -> np.array:
    print(features)
    df = spark.createDataFrame([[5, 6, 7]], ["col0", "col1", "col2"])
    df.show()
    return df


@UStep
def export_model(model: ssql.DataFrame, path: str, **kwargs) -> None:
    model.write.csv(path)


with UFlow("example_flow_spark") as flow:
    rand = get_rand()
    data = get_dataABC(rand=rand)
    target_variable = get_dataXYZ(rand=rand)
    data = clean_data(table=data)
    target_variable = clean_data(table=target_variable)
    features = generate_features(table1=data, table2=target_variable)
    model1 = train_model_RED(features=features)
    model2 = train_model_ROEY(features=features)
    export_model(model=model1, path="model1")
    export_model(model=model2, path="model2")

# flow.visualize()
# flow.run()
print(get_rand(ti="ti"))
