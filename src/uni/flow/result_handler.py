"""
Result Handlers to presist task results.

Anytime a task needs its output or inputs stored,
a result handler is used to determine where
this data should be stored (and how it can be retrieved).
"""
import os
from typing import Any

import cloudpickle
import mlflow
import pendulum
import prefect
from prefect.engine.result_handlers import ResultHandler
from slugify import slugify


# TODO Add parquet support for DF
# TODO Do we want to stay wth cloudpickle
class UResultHandler(ResultHandler):
    """
    Hook for storing and retrieving task results from local file storage.

    Task results are written using `cloudpickle` and stored in the
    provided location for use in future runs.

    Args:
        - dir (str, optional): the _absolute_ path to a directory for storing
            all results; defaults to `${prefect.config.home_dir}/results`
        - validate (bool, optional): a boolean specifying whether to validate
            the provided directory path; if `True`,
            the directory will be converted to an
            absolute path and created.  Defaults to `True`
    """

    def __init__(self, task_name, dir: str = None, validate: bool = True):
        """THE UResultHandler constructor."""
        full_prefect_path = os.path.abspath(prefect.config.home_dir)
        if (
            dir is None
            or os.path.commonpath([full_prefect_path, os.path.abspath(dir)])
            == full_prefect_path
        ):
            directory = os.path.join(prefect.config.home_dir, "results")
        else:
            directory = dir

        if validate:
            abs_directory = os.path.abspath(os.path.expanduser(directory))
            if not os.path.exists(abs_directory):
                os.makedirs(abs_directory)
        else:
            abs_directory = directory
        self.dir = abs_directory
        self.task_name = task_name
        super().__init__()

    def read(self, file_path: str) -> Any:
        """
        Read a result from the given file location.

        Args:
            - fpath (str): the _absolute_ path to
                the location of a written result

        Returns:
            - the read result from the provided file
        """
        self.logger.debug(
            "Starting to read result from {}...".format(file_path)
        )
        with open(file_path, "rb") as file:
            val = cloudpickle.loads(file.read())
        self.logger.debug(
            "Finished reading result from {}...".format(file_path)
        )
        return val

    def write(self, result: Any) -> str:
        """
        Serialize the provided result to local disk.

        Args:
            - result (Any): the result to write and store

        Returns:
            - str: the _absolute_ path to the written result on disk
        """
        file_name = (
                self.task_name
                + "-result-"
                + slugify(pendulum.now("utc").isoformat())
        )
        loc = os.path.join(self.dir, file_name)
        self.logger.debug("Starting to upload result to {}...".format(loc))
        with open(loc, "wb") as file:
            file.write(cloudpickle.dumps(result))
        self.logger.debug("Finished uploading result to {}...".format(loc))
        mlflow.log_artifact(loc, file_name)
        return loc
