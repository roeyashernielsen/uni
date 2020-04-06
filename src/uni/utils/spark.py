"""MLflow utils."""
from pyspark.sql import SparkSession


def get_spark_session(name=None, local=True):
    builder = SparkSession.builder
    if local:
        builder.master("local")
    else:
        builder.master("link to IS master")
    if name:
        builder.appName("test")

    builder.config("spark.sql.execution.arrow.enabled", "true") \
        .config("spark.sql.execution.arrow.fallback.enabled", "true") \
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "1000") \
        .config('spark.executor.memory', '2g')

    return builder.enableHiveSupport().getOrCreate()


spark = get_spark_session()
