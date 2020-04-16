"""MLflow utils."""
from ..utils import SparkEnv


def get_spark_session(spark_env, builder_func=None, conn_id=None):
    if spark_env.value == SparkEnv.Local.value:
        from pyspark.sql import SparkSession
        builder = SparkSession.builder
        if builder_func is not None:
            builder_func(builder)
        else:
            local_default_builder_func(builder)
        return builder.enableHiveSupport().getOrCreate()

    elif spark_env.value == SparkEnv.JupyterHub.value:
        import os
        from dss_airflow_utils.hooks.spark_hook import SparkHook
        os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"

        if builder_func is not None:
            _builder_func = builder_func
        else:
            _builder_func = spark_hook_default_builder_func

        if conn_id is not None:
            _conn_id = conn_id
        else:
            _conn_id = "mdl2_hive_metastore_prod"

        hook = SparkHook(builder_func=_builder_func, conn_id=_conn_id)
        return hook.get_spark_session()

    else:
        raise ValueError(f"spark_env must be a SparkEnv but got {type(spark_env)}")


def spark_hook_default_builder_func(builder):
    builder.appName("DevelopingSparkInJupyter")
    builder.config("spark.executor.memory", "4G")
    # add more spark configuration properties as needed
    # (see https://spark.apache.org/docs/latest/configuration.html for other configs)
    builder.config("spark.dynamicAllocation.maxExecutors", "2")
    # Set the number of latest rolling log files that are going to be retained by the system
    # to 1. Older log files will be deleted. Saves cloud space.
    builder.config("spark.executor.logs.rolling.maxRetainedFiles", 1)
    base_default_builder_func(builder)


def local_default_builder_func(builder):
    builder.master("local")
    builder.appName("DevelopingSparkInLocal")
    builder.config("spark.executor.memory", "2G")
    base_default_builder_func(builder)


def base_default_builder_func(builder):
    builder.config("spark.sql.execution.arrow.enabled", "true")
    builder.config("spark.sql.execution.arrow.fallback.enabled", "true")
