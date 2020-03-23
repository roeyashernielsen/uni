"""UNI Step class."""
import functools

import mlflow
import prefect
from src.uni.io import writer

TASK_PREFIX = "ustep_"


class UStep:
    """UNI step decorator."""

    def __init__(self, func):
        """UNI Step constructor."""
        functools.update_wrapper(self, func)
        self.func = func
        self.name = TASK_PREFIX + str(func.__name__)

    def __call__(self, **kwargs):
        """Decorator."""
        return self.__mlflow_wrapper(nested=False, **kwargs)

    def __get_run_id(self):
        """Get task's MLflow run_id."""
        if "runs" in prefect.context.keys():
            if self.name in prefect.context.runs.keys():
                return prefect.context.runs[self.name]
        return None

    # TODO: are we ok with artifacts in the params dict?
    def __get_runs_md(self):
        params = {}
        # artifacts = {}
        if "runs" in prefect.context.keys():
            for task_run_id in prefect.context.runs.values():
                run_md = mlflow.tracking.MlflowClient().get_run(task_run_id)
                params = {**params, **run_md.data.params}
                # artifacts.update({task_name: run_md.info.artifact_uri})
        # return {"params": params, "artifacts": artifacts}
        return params

    def __mlflow_wrapper(self, nested=None, **kwargs):
        """Start mlflow run before exec the function."""
        with prefect.context(runs_md=self.__get_runs_md()):
            with mlflow.start_run(
                    run_id=self.__get_run_id(), run_name=self.name, nested=nested
            ):
                for key, value in kwargs.items():
                    mlflow.log_param(f"input_{key}", value)
                func_return = self.func(**kwargs)
                writer.save_obj(func_return, self.name + "_return")

        return func_return

    def step(self, run_id=None, **kwargs):
        """Step."""

        @prefect.task(name=self.name)
        @functools.wraps(self.func)
        def wrapper(**kwargs):
            return self.__mlflow_wrapper(nested=True, **kwargs)

        return wrapper(**kwargs)
