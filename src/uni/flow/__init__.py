"""Submodule containing flow utils."""


def is_primitive(obj):
    """Check if the object is "primitive"."""
    return not hasattr(obj, "__dict__")
