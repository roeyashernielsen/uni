"""Top-level module for UNI."""
import prefect


def get_logger(): return prefect.context.get("logger")
