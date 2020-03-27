"""Top-level module for UNI."""
import prefect


def get_logger():
    """Get the prefect logger."""
    return prefect.context.get("logger")
