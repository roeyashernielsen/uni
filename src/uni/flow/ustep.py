"""UNI Step class."""

import functools

import mlflow
import prefect

from ..io import writer
from . import get_params_from_pre_tasks, is_primitive
from .result_handler import UResultHandler


class UStep:
    """UNI step class."""

    def __init__(self, func):
        """UNI Step constructor."""
        functools.update_wrapper(self, func)
        self.func = func
        self._name = self.func.__name__

    def __call__(self, **kwargs):
        """Trigger the step as a regular function with MLflow wrapping."""
        return self.__mlflow_wrapper(nested=False, **kwargs)

    def run(self, **kwargs):
        """Trigger the step as a regular function."""
        return self.func(**kwargs)

    def __mlflow_wrapper(self, nested=None, airflow_step=False, **kwargs):
        """Start MLflow run and log the input/output."""
        if airflow_step and "mlflow_run_id" in kwargs:
            mlflow.start_run(run_id=kwargs.pop("mlflow_run_id"))

        run = mlflow.start_run(run_name=self._name, nested=nested)

        for key, value in kwargs.items():
            if is_primitive(value):
                mlflow.log_param(f"input_param.{key}", value)
            else:
                mlflow.log_param(f"input_param.{key}", type(value))

        func_return = self.func(**kwargs)

        if is_primitive(func_return):
            mlflow.log_param(f"return_value", func_return)

        if airflow_step:
            writer.save_obj(func_return, self._name)
            mlflow.end_run()
            mlflow.end_run()
            return run.info.run_id
        else:
            return func_return

    def step(self, **kwargs):
        """The step decorator."""

        @prefect.task(
            name=self._name,
            checkpoint=True,
            result_handler=UResultHandler(self._name),
        )
        @functools.wraps(self.func)
        def wrapper(**kwargs):
            return self.__mlflow_wrapper(nested=True, **kwargs)

        return wrapper(**kwargs)

    def airflow_step(self, **kwargs):
        """The step decorator."""
        name = kwargs.get("name", None)
        if name is not None:
            self._name = name

        @functools.wraps(self.func)
        def wrapper(**kwargs):
            return self.__mlflow_wrapper(
                nested=True, airflow_step=True, **get_params_from_pre_tasks(**kwargs)
            )

        return wrapper(**kwargs)
