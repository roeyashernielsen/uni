"""MLflow utils."""
from ..utils import SparkEnv


def get_spark_session(spark_env, **kwargs):
    if spark_env.value == SparkEnv.Local.value:
        from pyspark.sql import SparkSession
        builder = SparkSession.builder
        builder.master("local")
        builder.appName("DevelopingSparkInLocal")
        builder.config("spark.executor.memory", "2G")
        builder.config("spark.sql.execution.arrow.enabled", "true")
        builder.config("spark.sql.execution.arrow.fallback.enabled", "true")
        return builder.enableHiveSupport().getOrCreate()

    elif spark_env.value == SparkEnv.JupyterHub.value:
        import os
        from dss_airflow_utils.hooks.spark_hook import SparkHook
        os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
        hook = SparkHook(builder_func=builder_func, conn_id="mdl2_hive_metastore_prod")
        return hook.get_spark_session()
    elif spark_env.value == SparkEnv.Recipe.value:
        return kwargs.get("spark", None)
    else:
        raise ValueError(f"spark_env must be a SparkEnv but got {type(spark_env)}")


def builder_func(builder):
    builder.appName("DevelopingSparkInJupyter")
    builder.config("spark.executor.memory", "2G")
    builder.config("spark.sql.execution.arrow.enabled", "true")
    builder.config("spark.sql.execution.arrow.fallback.enabled", "true")
    builder.config("spark.sql.execution.arrow.maxRecordsPerBatch", "1000")
    # add more spark configuration properties as needed
    # (see https://spark.apache.org/docs/latest/configuration.html for other configs)
    builder.config("spark.dynamicAllocation.maxExecutors", "1")
    # Set the number of latest rolling log files that are going to be retained by the system
    # to 1. Older log files will be deleted. Saves cloud space.
    builder.config("spark.executor.logs.rolling.maxRetainedFiles", 1)
