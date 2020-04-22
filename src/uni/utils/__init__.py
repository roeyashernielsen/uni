"""Submodule containing general utils."""
from enum import Enum


class ExtendedEnum(Enum):
    @classmethod
    def tolist(cls):
        return list(map(lambda c: c.value, cls))


class SparkEnv(ExtendedEnum):
    Local = "Local"
    JupyterHub = "SparkHook"
    Recipe = "AirflowSparkOperator"
