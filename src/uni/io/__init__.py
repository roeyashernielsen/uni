"""Submodule containing io utils."""
from enum import Enum


class ExtendedEnum(Enum):

    @classmethod
    def tolist(cls):
        return list(map(lambda c: c.value, cls))


class TabularFileFormats(ExtendedEnum):
    Parquet = ".parquet"
    Feather = ".feather"
    # TODO to impl
    # JSON = ".json"
    # CSV = ".csv"


class PyObjFileFormat(ExtendedEnum):
    CloudPickle = ".cpkl"
    Pickle = ".pkl"
    # # TODO to impl
    # JSON = ".json"
    # YAML = ".yaml"
