"""Writers utils."""


def save_obj(obj, path=None, format=None, mode=None, **options):
    """
    Saves the contents of the :class:`DataFrame` to a data source.
    In addition, log the file or directory as an artifact of the currently MLflow active run.
    If no run is active, this method will create a new active run.
    """
    pass


def save_df(df, path=None, format=None, mode=None, partitionBy=None, **options):
    """
    Saves the contents of the :class:`DataFrame` to a data source.
    In addition, log the file or directory as an artifact of the currently MLflow active run.
    If no run is active, this method will create a new active run.
    """
    pass
