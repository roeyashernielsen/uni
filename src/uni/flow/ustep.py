"""UNI Step class."""
import functools

import mlflow

from prefect import task


def mlflow_wrapper(func, run_id=None, run_name=None):
    """MLflow Wrapper."""

    @functools.wraps(func)
    def wrapper(**kwargs):
        with mlflow.start_run(run_id=run_id, run_name=run_name):
            func(**kwargs)

    return wrapper


class UStep:
    """Ustep decorator."""

    def __init__(self, func):
        functools.update_wrapper(self, func)
        self.func = mlflow_wrapper(func)

    def __call__(self, **kwargs):
        return self.func(**kwargs)

    def step(self, **kwargs):
        @task
        @functools.wraps(self.func)
        def wrapper(**kwargs):
            return self.func(**kwargs)

        return wrapper(**kwargs)
