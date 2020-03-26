"""UNI Step class."""
import functools
import inspect

import mlflow
import prefect
from prefect.engine.result_handlers import LocalResultHandler

from ..io import writer, reader
from .result_handler import UResultHandler

TASK_PREFIX = "ustep_"


def is_primitive(obj):
    return not hasattr(obj, '__dict__')


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

    def __get_req_params(self):
        result = {}
        flow = prefect.context.get("uflow", None)
        if flow.flow:
            for edge in flow.flow.edges:
                if edge.downstream_task.name == self.name:
                    result.update({edge.upstream_task.name: edge.key})
        return result

    def __fetch_req_params(self, req_params):
        result = {}
        for func_name, key in req_params.items():
            result.update({key: reader.load_obj(f"{func_name}_return")})
        return result

    def __get_func_params(self, params):
        result = {}
        if params:
            for param in inspect.signature(self.func).parameters:
                if param in params:
                    result.update({param: params[param]})
        return result

    def __get_params(self):
        req_params = self.__get_req_params()
        if req_params:
            fetched_params = self.__fetch_req_params(req_params)
            if fetched_params:
                return self.__get_func_params(fetched_params)

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
                    mlflow.log_param(f"input_param-{key}", value)
                func_return = self.func(**kwargs)
                if is_primitive(func_return):
                    mlflow.log_param(f"return_value", func_return)

        return func_return

    def step(self, run_id=None, **kwargs):
        """Step."""

        @prefect.task(name=self.name, checkpoint=True, result_handler=UResultHandler(self.name))
        @functools.wraps(self.func)
        def wrapper(**kwargs):
            return self.__mlflow_wrapper(nested=True, **kwargs)

        return wrapper(**kwargs)
