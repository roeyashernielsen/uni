"""Submodule containing flow utils."""
import mlflow

from ..utils.mlflow import load_artifact

from ..utils import ExtendedEnum


class UStepType(ExtendedEnum):
    Python = "PythonOperator"
    Spark = "SparkOperator"


def is_primitive(obj):
    """Check if object is "primitive"."""
    return not hasattr(obj, "__dict__")


# def get_runs_params(task_instance, func_param, **kwargs):
#     """Get return value from MLflow run object."""
#     result = {}
#     for func_name, param in func_param.items():
#         run_id = task_instance.xcom_pull(task_ids=func_name)
#         result.update({param: load_artifact(run_id, func_name, kwargs)})
#     print("result=" + str(result))
#     return result


def get_params(**kwargs):
    """Get return values from prev-functions."""
    if "ti" in kwargs:
        task_instance = kwargs["ti"]
    else:
        raise Exception("Couldn't find ti in kwargs")
    mlflow.set_tracking_uri(task_instance.xcom_pull(key="mlflow_tracking_uri"))
    mlflow_run_id = {"mlflow_run_id": task_instance.xcom_pull(key="mlflow_run_id")}
    params = kwargs.get("params", None)
    if params is not None:
        kwargs = {**kwargs, **params}
    func_param = kwargs.get("func_param", {})
    const_params = kwargs.get("const_params", {})
    runs_params = {}
    for func_name, param in func_param.items():
        run_id = task_instance.xcom_pull(task_ids=func_name)
        runs_params.update({param: load_artifact(run_id, func_name, **kwargs)})
    return {**mlflow_run_id, **const_params, **runs_params}


def init_step(**kwargs):
    """Airflow init step."""
    from dss_airflow_utils.workspace_utils import path_in_workspace

    mlflow_tracking_uri = "file:" + path_in_workspace("") + "mlruns"
    mlflow.set_tracking_uri(mlflow_tracking_uri)
    kwargs["ti"].xcom_push(key="mlflow_tracking_uri", value=mlflow_tracking_uri)
    run = mlflow.start_run(run_name=f'{kwargs["dag"].dag_id}_{kwargs["run_id"]}')
    mlflow.end_run()
    kwargs["ti"].xcom_push(key="mlflow_run_id", value=run.info.run_id)
