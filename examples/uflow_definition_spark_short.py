import random

from uni.flow import UStepType
from uni.flow.ustep import UStep


@UStep
def get_rand(**kwargs) -> int:
    rand = random.randint(0, 100)
    return rand


@UStep(step_type=UStepType.Spark)
def train_model_RED(rand, **kwargs):
    df = spark.createDataFrame([[rand, rand * 2, rand * 3]], ["col0", "col1", "col2"])
    df.show()
    return df


@UStep(step_type=UStepType.Spark)
def train_model_ROEY(rand, **kwargs):
    df = spark.createDataFrame([[rand, rand * 2, rand * 3]], ["col0", "col1", "col2"])
    df.show()
    return df


@UStep(step_type=UStepType.Spark)
def export_model(df, path: str, **kwargs):
    df.show()
    df.write.mode("overwrite").parquet(path)


with UFlow("example_flow_spark") as flow:
    rand = get_rand()
    model1 = train_model_RED(rand=rand)
    model2 = train_model_ROEY(rand=rand)
    export_model(df=model1, path="model1")
    export_model(df=model2, path="model2")
