import random

import numpy as np
import pandas as pd

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


@UStep
def train_model_RED(features: pd.DataFrame, **kwargs) -> np.array:
    arr = pd.DataFrame([1, 3, 5, 7], columns=["X"])
    return arr


@UStep
def train_model_ROEY(features: pd.DataFrame, **kwargs) -> np.array:
    arr = pd.DataFrame([2, 4, 6, 8], columns=["X"])
    return arr


@UStep
def export_model(model: np.array, path: str, **kwargs) -> None:
    model.to_csv(path, index=False)


with UFlow("example_flow") as flow:
    rand = get_rand()
    data = get_dataABC(rand=rand)
    target_variable = get_dataXYZ(rand=rand)
    data = clean_data(table=data)
    target_variable = clean_data(table=target_variable)
    features = generate_features(table1=data, table2=target_variable)
    model1 = train_model_RED(features=features)
    model2 = train_model_ROEY(features=features)
    export_model(model=model1, path="model1.csv")
    export_model(model=model2, path="model2.csv")
