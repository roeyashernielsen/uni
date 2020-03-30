"""Submodule containing flow utils."""
import mlflow

from ..io.reader import load_obj


def is_primitive(obj):
    """Check if the object is "primitive"."""
    return not hasattr(obj, "__dict__")


def get_runs_params(task_instance, func_param):
    """Get return value from MLflow run object."""
    result = {}
    for func_name, param in func_param:
        run_id = task_instance.xcom_pull(task_ids=func_name)
        run_md = mlflow.tracking.MlflowClient().get_run(run_id)
        value = run_md.data.params.get(func_name, None)
        if isinstance(value, str) and value.startswith("file://"):
            result = result.update({param: load_obj(value, func_name)})
    return result


def get_params_from_pre_tasks(**kwargs):
    """Get return values from prev-functions."""
    if "ti" in kwargs:
        task_instance = kwargs["ti"]
    else:
        raise Exception("Couldn't find ti in kwargs")

    func_param = kwargs.get("func_param", None)
    if func_param:
        return get_runs_params(task_instance, func_param)
    return {}
