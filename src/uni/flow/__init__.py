"""Submodule containing flow utils."""
import mlflow

from ..utils import ExtendedEnum


class UStepType(ExtendedEnum):
    Python = "PythonOperator"
    Spark = "SparkOperator"


class FlowType(ExtendedEnum):
    NA = None
    Prefect = "Prefect"
    Airflow = "Airflow"


def is_primitive(obj):
    """Check if object is "primitive"."""
    return not hasattr(obj, "__dict__")


def init_step(**kwargs):
    """Airflow init step."""
    from dss_airflow_utils.workspace_utils import path_in_workspace

    mlflow_tracking_uri = "file:" + path_in_workspace("") + "mlruns"
    mlflow.set_tracking_uri(mlflow_tracking_uri)
    kwargs["ti"].xcom_push(key="mlflow_tracking_uri", value=mlflow_tracking_uri)
    run = mlflow.start_run(run_name=f'{kwargs["dag"].dag_id}_{kwargs["run_id"]}')
    mlflow.end_run()
    kwargs["ti"].xcom_push(key="mlflow_run_id", value=run.info.run_id)
