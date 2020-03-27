"""UNI Step class."""
import functools

import mlflow
import prefect

from . import is_primitive
from .result_handler import UResultHandler


class UStep:
    """UNI step class."""

    def __init__(self, func):
        """UNI Step constructor."""
        functools.update_wrapper(self, func)
        self.func = func
        self.name = self.func.__name__

    def __call__(self, **kwargs):
        """Trigger the step as a regular function with MLflow wrapping."""
        return self.__mlflow_wrapper(nested=False, **kwargs)

    def run(self, **kwargs):
        """Trigger the step as a regular function."""
        return self.func(**kwargs)

    def __mlflow_wrapper(self, nested=None, **kwargs):
        """Start MLflow run and log the input/output."""
        with mlflow.start_run(run_name=self.name, nested=nested):
            for key, value in kwargs.items():
                mlflow.log_param(f"input_param-{key}", value)
            func_return = self.func(**kwargs)
            if is_primitive(func_return):
                mlflow.log_param(f"return_value", func_return)

        return func_return

    def step(self, **kwargs):
        """The step decorator."""

        @prefect.task(
            name=self.name,
            checkpoint=True,
            result_handler=UResultHandler(self.name),
        )
        @functools.wraps(self.func)
        def wrapper(**kwargs):
            return self.__mlflow_wrapper(nested=True, **kwargs)

        return wrapper(**kwargs)
