"""Submodule containing io utils."""
from ..utils import ExtendedEnum


class TabularFileFormats(ExtendedEnum):
    Parquet = ".parquet"
    Feather = ".feather"
    # TODO need to impl
    # JSON = ".json"
    # CSV = ".csv"


class PyObjFileFormat(ExtendedEnum):
    CloudPickle = ".cpkl"
    Pickle = ".pkl"
    # # TODO need to impl
    # JSON = ".json"
    # YAML = ".yaml"


class ObjType(ExtendedEnum):
    PandasDF = "PandasDataFrame"
    SparkDF = "SparkDataFrame"
    PyObj = "PyObj"


class PathType(ExtendedEnum):
    File = "file"
    Directory = "directory"
