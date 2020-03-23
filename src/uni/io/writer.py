"""Writers utils."""
import os
import pickle
import tempfile

import mlflow


def save_obj(obj, name, path=None, **options):
    """
    Saves the contents of the :class:`DataFrame` to a data source.

    In addition, log the file or directory as an artifact
    of the currently MLflow active run.
    If no run is active, this method will create a new active run.
    """
    if path is None:
        path = tempfile.mkdtemp()

    file_path = os.path.join(path, name + ".pkl")

    with open(file_path, "wb") as f:
        pickle.dump(obj, f)

    mlflow.log_artifact(file_path, name)
    mlflow.log_param(name + "_path", mlflow.get_artifact_uri(name))


def save_df(
        df, path=None, format=None, mode=None, partition_by=None, **options
):
    """
    Saves the contents of the :class:`DataFrame` to a data source.

    In addition, log the file or directory as an artifact
    of the currently MLflow active run.
    If no run is active, this method will create a new active run.
    """
