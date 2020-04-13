"""MLflow utils."""
from uni.utils import SparkEnv


def get_spark_session(spark_env, **kwargs):
    if spark_env.value == SparkEnv.Local.value:
        from pyspark.sql import SparkSession
        builder_func(SparkSession.builder)
        return SparkSession.builder.getOrCreate()

    elif spark_env.value == SparkEnv.JupyterHub.value:
        import os
        from dss_airflow_utils.hooks.spark_hook import SparkHook
        os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
        hook = SparkHook(builder_func=builder_func, conn_id="mdl2_hive_metastore_prod")
        return hook.get_spark_session()
    else:
        raise ValueError(f"spark_env must be a SparkEnv but got {type(spark_env)}")


def builder_func(builder, env=SparkEnv.JupyterHub):
    builder.config("spark.executor.memory", "2G")
    builder.config("spark.sql.execution.arrow.enabled", "true")
    builder.config("spark.sql.execution.arrow.fallback.enabled", "true")
    builder.config("spark.sql.execution.arrow.maxRecordsPerBatch", "1000")
    # add more spark configuration properties as needed
    # (see https://spark.apache.org/docs/latest/configuration.html for other configs)
    if env == SparkEnv.JupyterHub:
        builder.appName("DevelopingSparkInJupyter")
        builder.config("spark.dynamicAllocation.maxExecutors", "1")
        # Set the number of latest rolling log files that are going to be retained by the system
        # to 1. Older log files will be deleted. Saves cloud space.
        builder.config("spark.executor.logs.rolling.maxRetainedFiles", 1)
    elif env == SparkEnv.Local:
        builder.master("local")
        builder.appName("DevelopingSparkInLocal")
        builder.enableHiveSupport()
    else:
        raise ValueError(f"env must be a SparkEnv but got {type(env)}")
