"""Submodule containing flow utils."""
import mlflow

from ..io.reader import load_obj


def is_primitive(obj):
    """Check if the object is "primitive"."""
    return not hasattr(obj, "__dict__")


def get_runs_params(task_instance, func_param):
    """Get return value from MLflow run object."""
    result = {"mlflow_run_id": task_instance.xcom_pull(key="mlflow_run_id")}
    for func_name, param in func_param.items():
        run_id = task_instance.xcom_pull(task_ids=func_name)
        run_md = mlflow.get_run(run_id)
        value = run_md.data.params.get(func_name, None)
        if isinstance(value, str) and value.startswith("file://"):
            result.update({param: load_obj(value, func_name)})
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
    run = mlflow.start_run(run_name=f'{kwargs["dag"].dag_id}_{kwargs["run_id"]}')
    mlflow.end_run()
    kwargs['ti'].xcom_push(key='mlflow_run_id', value=run.info.run_id)
