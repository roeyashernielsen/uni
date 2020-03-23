"""Reader utils."""
import pickle
from urllib.parse import unquote, urlparse

import prefect


def load_obj(name=None, path=None, **options):
    """
    Loads object.

    Loads data from a data source
    and returns it as a :class:`DataFrame`.
    If name is not None, load the df from the MLflow artifact
    """
    if path is None:
        path = prefect.context.runs_md[name + "_path"]
    with open(unquote(urlparse(path + f"/{name}.pkl").path), "rb") as file:
        data = pickle.load(file)
    return data

# def load_df(name=None, path=None, format=None, schema=None, **options):
#     """
#     Loads Dataframe.
#
#     Loads data from a data source
#     and returns it as a :class:`DataFrame`.
#     If name is not None, load the df from the MLflow artifact
#     """
