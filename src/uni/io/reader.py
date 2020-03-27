"""Reader utils."""
import pickle
from urllib.parse import unquote, urlparse

import prefect


def load_obj(name, file_path=None, **options):
    """
    Loads object.

    Loads data from a data source
    and returns it as a :class:`DataFrame`.
    If name is not None, load the df from the MLflow artifact
    """
    if file_path is None:
        if "runs_md" in prefect.context.keys():
            if name in prefect.context.runs_md:
                with open(
                    unquote(
                        urlparse(
                            prefect.context.runs_md[name] + f"/{name}.pkl"
                        ).path
                    ),
                    "rb",
                ) as file:
                    return pickle.load(file)
        return Exception
    else:
        with open(file_path, "rb") as file:
            return pickle.load(file)


# def load_df(name=None, path=None, format=None, schema=None, **options):
#     """
#     Loads Dataframe.
#
#     Loads data from a data source
#     and returns it as a :class:`DataFrame`.
#     If name is not None, load the df from the MLflow artifact
#     """
