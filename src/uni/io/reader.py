"""Reader utils."""
import pickle
from urllib.parse import unquote, urlparse


def load_obj(name, file_path):
    """
    Loads object.

    Loads data from a data source
    and returns it as a :class:`DataFrame`.
    If name is not None, load the df from the MLflow artifact
    """
    with open(unquote(urlparse(f"{file_path}/{name}.pkl").path), "rb") as file:
        return pickle.load(file)


# def load_df(name=None, path=None, format=None, schema=None, **options):
#     """
#     Loads Dataframe.
#
#     Loads data from a data source
#     and returns it as a :class:`DataFrame`.
#     If name is not None, load the df from the MLflow artifact
#     """
