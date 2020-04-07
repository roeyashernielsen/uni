"""Submodule containing flow utils."""
import mlflow

from ..utils.mlflow import load_artifact


def is_primitive(obj):
    """Check if object is "primitive"."""
    return not hasattr(obj, "__dict__")


def get_runs_params(task_instance, func_param):
    """Get return value from MLflow run object."""
    result = {"mlflow_run_id": task_instance.xcom_pull(key="mlflow_run_id")}
    for func_name, param in func_param.items():
        run_id = task_instance.xcom_pull(task_ids=func_name)
        result.update({param: load_artifact(run_id, func_name)})
    return result


def get_params_from_pre_tasks(**kwargs):
    """Get return values from prev-functions."""
    if "ti" in kwargs:
        task_instance = kwargs["ti"]
    else:
        raise Exception("Couldn't find ti in kwargs")
    func_param = kwargs.get("func_param", {})
    const_params = kwargs.get("const_params", {})
    return {**const_params, **get_runs_params(task_instance, func_param)}


def init_step(**kwargs):
    """Airflow init step."""
    run = mlflow.start_run(run_name=f'{kwargs["dag"].dag_id}_{kwargs["run_id"]}')
    mlflow.end_run()
    kwargs["ti"].xcom_push(key="mlflow_run_id", value=run.info.run_id)
