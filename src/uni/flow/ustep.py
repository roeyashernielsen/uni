"""UNI Step class."""

import functools

import mlflow

from ..io import writer
from . import get_params, is_primitive, UStepType
from ..utils import SparkEnv
from ..utils.spark import get_spark_session


class _UStep:
    """UNI step class."""

    def __init__(self, func, step_type=UStepType.Python):
        """UNI Step constructor."""
        functools.update_wrapper(self, func)
        self.func = func
        self.name = self.func.__name__
        self.step_type = step_type

    def __call__(self, spark_env=SparkEnv.Local, airflow_step=False, mlflow_tracking=True, **kwargs):
        """Trigger the step as a regular function with MLflow wrapping."""
        prefect_flow = False
        if airflow_step:
            mlflow_tracking = True
            spark_env = SparkEnv.Recipe
        else:
            import prefect
            flow = prefect.context.get("flow", None)
            if flow:
                mlflow_tracking = True
                prefect_flow = True
                if type(flow.environment).__name__ == 'JupyterHubEnvironment':
                    spark_env = SparkEnv.JupyterHub
            elif not mlflow_tracking:
                mlflow_tracking = False
        print("triggering ustep _call_ with spark_evn=" + str(spark_env))
        return self.__run(prefect_flow, airflow_step, mlflow_tracking, spark_env, **kwargs)

    def __run(self, prefect_flow, airflow_step, mlflow_tracking, spark_env, **kwargs):
        """Run the function."""
        func = self.func
        if airflow_step:
            func = self.__airflow_step_wrapper(func=func, **kwargs)
            func = self.__mlflow_wrapper(func=func, nested=True, airflow_step=True, **kwargs)
        elif prefect_flow:
            func = self.__prefect_step_wrapper(func=func, **kwargs)
            func = self.__mlflow_wrapper(func=func, nested=True, **kwargs)
        elif mlflow_tracking:
            func = self.__mlflow_wrapper(func=func, nested=False, **kwargs)

        if self.step_type.value == UStepType.Spark.value:
            func = self.__spark_wrapper(func=func, spark_env=spark_env, **kwargs)

        return func(**kwargs)

    def __mlflow_wrapper(self, func, nested=None, airflow_step=False, **kwargs):
        """Start MLflow run and log the input/output."""

        @functools.wraps(func)
        def mlflow_wrapper(**kwargs):
            with mlflow.start_run(run_name=self.name, nested=nested) as run:
                for key, value in kwargs.items():
                    if is_primitive(value):
                        mlflow.log_param(f"input_param.{key}", value)
                    else:
                        mlflow.log_param(f"input_param.{key}", type(value))

                func_return = func(**kwargs)

                if is_primitive(func_return):
                    mlflow.log_param(f"return_value", func_return)

                if airflow_step:
                    writer.save(obj=func_return, name=self.name, mlflow_logging=True)
                    return run.info.run_id
                else:
                    return func_return

        return mlflow_wrapper

    def __spark_wrapper(self, func, spark_env=SparkEnv.Local, **kwargs):
        """Start SparkSession as spark."""

        @functools.wraps(func)
        def spark_wrapper(**kwargs):
            func_globals = func.__globals__
            sentinel = object()
            old_value = func_globals.get('spark', sentinel)
            func_globals['spark'] = get_spark_session(spark_env=spark_env, **kwargs)

            try:
                func_result = func(**kwargs)
            finally:
                if old_value is sentinel:
                    del func_globals['spark']
                else:
                    func_globals['spark'] = old_value

            return func_result

        return spark_wrapper

    def __prefect_step_wrapper(self, func, **kwargs):
        """The step decorator."""
        import prefect
        from .result_handler import UResultHandler

        @prefect.task(
            name=self.name, checkpoint=True, result_handler=UResultHandler(self.name),
        )
        @functools.wraps(func)
        def prefect_wrapper(**kwargs):
            return func(**kwargs)

        return prefect_wrapper

    def __airflow_step_wrapper(self, func, **kwargs):
        """The step decorator."""

        @functools.wraps(func)
        def airflow_wrapper(**kwargs):
            name = kwargs.get("name", None)
            if name is not None:
                self.name = name
            print("kwargs=" + str(kwargs))
            params = get_params(**kwargs)
            print("params=" + str(params))
            if "mlflow_run_id" in params:
                mlflow.start_run(run_id=params.pop("mlflow_run_id"))
            run_id = func(**{**kwargs, **params})
            mlflow.end_run()
            return run_id

        return airflow_wrapper


# wrap _UStep to allow for deferred calling
def UStep(_func=None, step_type=UStepType.Python):
    def decorator_UStep(func):

        @functools.wraps(func)
        def ustep_wrapper(**kwargs):
            airflow_step = False
            spark_env = SparkEnv.Local
            ustep = _UStep(func, step_type=step_type)
            if kwargs.get("get_step_type", False):
                return step_type.value
            if kwargs.get("spark", None) is not None:
                spark_env = SparkEnv.Recipe
                airflow_step = True
            if "ti" in kwargs:
                airflow_step = True
            return ustep(spark_env=spark_env, airflow_step=airflow_step, **kwargs)

        return ustep_wrapper

    if _func is None:
        return decorator_UStep
    else:
        return decorator_UStep(_func)
