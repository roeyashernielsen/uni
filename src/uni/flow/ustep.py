"""UNI Step class."""

from functools import wraps, partial

import mlflow
from pyspark.sql import SparkSession

from .. import shared
from ..io import writer
from . import is_primitive, UStepType, FlowType
from ..utils import SparkEnv
from ..utils.mlflow import load_artifact
from ..utils.spark import get_spark_session


def UStep(func=None, *, name=None, step_type=None, spark_env=None):
    """UNI step decorator."""

    if func is None:
        return partial(UStep, name=name, step_type=step_type, spark_env=spark_env)

    name = name if name else func.__name__
    step_type = step_type if step_type else UStepType.Python
    spark_env = spark_env if spark_env else SparkEnv.Local
    flow_type = FlowType.NA
    task_instance = None

    @wraps(func)
    def ustep_wrapper(*args, **kwargs):
        """Trigger the step ."""
        nonlocal name
        nonlocal flow_type
        nonlocal task_instance
        nonlocal spark_env

        kwargs = {**kwargs, **kwargs.get('context', {})}

        task_instance = kwargs.get("ti", None)
        if task_instance is not None:
            flow_type = FlowType.Airflow
            if step_type.value == UStepType.Spark.value:
                if args:
                    if isinstance(args[0], SparkSession):
                        spark_env = SparkEnv.Recipe
                        shared.spark_session = args[0]
        else:
            import prefect
            flow = prefect.context.get("flow", None)
            if flow:
                flow_type = FlowType.Prefect
                if step_type.value == UStepType.Spark.value:
                    if type(flow.environment).__name__ == 'JupyterHubEnvironment':
                        spark_env = SparkEnv.JupyterHub

            if step_type.value == UStepType.Spark.value:
                shared.spark_session = get_spark_session(spark_env)

        return __run(func, **kwargs)

    def __run(_func, **kwargs):
        """Run the function."""
        if step_type.value == UStepType.Spark.value:
            _func = __spark_wrapper(_func)

        if flow_type.value == FlowType.Airflow.value:
            _func = __airflow_step_wrapper(_func)
        elif flow_type.value == FlowType.Prefect.value:
            _func = __prefect_step_wrapper(_func)

        return _func(**kwargs)

    def __mlflow_wrapper(_func):
        """Start MLflow run and log the input/output."""

        @wraps(_func)
        def mlflow_wrapper(**kwargs):
            print("enter mlflow_wrapper")
            print("flow_type=" + str(flow_type))
            print("step_type=" + str(step_type))
            print("spark_env=" + str(spark_env))
            with mlflow.start_run(run_name=name, nested=True) as run:
                for key, value in kwargs.items():
                    if is_primitive(value):
                        mlflow.log_param(f"input_param.{key}", value)
                    else:
                        mlflow.log_param(f"input_param.{key}", type(value))

                func_return = _func(**kwargs)

                if is_primitive(func_return):
                    mlflow.log_param(f"return_value", func_return)

                if flow_type.value == FlowType.Airflow.value:
                    path = None
                    if step_type.value == UStepType.Spark.value:
                        from dss_airflow_utils.workspace_utils import path_in_workspace
                        path = path_in_workspace("") + "tmp"
                    writer.save(obj=func_return, name=name, dir_path=path, mlflow_logging=True)
                    return run.info.run_id
                else:
                    return func_return

        return mlflow_wrapper

    def __spark_wrapper(_func):
        """Start SparkSession as spark."""

        @wraps(_func)
        def spark_wrapper(**kwargs):
            print("enter spark_wrapper")
            print("flow_type=" + str(flow_type))
            print("step_type=" + str(step_type))
            print("spark_env=" + str(spark_env))
            func_globals = func.__globals__
            sentinel = object()
            old_value = func_globals.get('spark', sentinel)
            func_globals['spark'] = shared.spark_session

            try:
                func_result = _func(**kwargs)
            finally:
                if old_value is sentinel:
                    del func_globals['spark']
                else:
                    func_globals['spark'] = old_value

            return func_result

        return spark_wrapper

    def __prefect_step_wrapper(_func):
        """The step decorator."""
        import prefect
        from .result_handler import UResultHandler

        @prefect.task(
            name=name, tags={step_type.value}, checkpoint=True, result_handler=UResultHandler(name),
        )
        @wraps(_func)
        def prefect_wrapper(**kwargs):
            return __mlflow_wrapper(_func)(**kwargs)

        return prefect_wrapper

    def __airflow_step_wrapper(_func):
        """The step decorator."""

        @wraps(_func)
        def airflow_wrapper(**kwargs):
            print("enter airflow_wrapper")
            print("flow_type=" + str(flow_type))
            print("step_type=" + str(step_type))
            print("spark_env=" + str(spark_env))
            mlflow.set_tracking_uri(task_instance.xcom_pull(key="mlflow_tracking_uri"))
            mlflow_run_id = task_instance.xcom_pull(key="mlflow_run_id")

            params = kwargs.get("params", None)
            if params is not None:
                kwargs = {**kwargs, **params}

            func_param = kwargs.get("func_param", {})
            const_params = kwargs.get("const_params", {})
            runs_params = {}

            for func_name, param in func_param.items():
                task_run_id = task_instance.xcom_pull(task_ids=func_name)
                runs_params.update({param: load_artifact(task_run_id, func_name)})

            mlflow.start_run(run_id=mlflow_run_id)
            run_id = __mlflow_wrapper(_func)(**{**const_params, **runs_params})
            mlflow.end_run()
            return run_id

        return airflow_wrapper

    return ustep_wrapper
