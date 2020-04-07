"""Submodule containing io utils."""
from enum import Enum
import pandas as pd
import pyspark.sql as ssql


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


class ObjType(ExtendedEnum):
    PandasDF = "PandasDataFrame"
    SparkDF = "SparkDataFrame"
    PyObj = "PyObj"


class PathType(ExtendedEnum):
    File = "file"
    Directory = "directory"
