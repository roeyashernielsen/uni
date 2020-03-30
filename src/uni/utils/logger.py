"""
Logging utilities for both developers and users of Uni. This module defines a
simplified API for logging; the goal is to make it easy to get started with logging.

Example usage:

.. code:: python

    from ..util import logger

    logger.set_file_sink("name_of_log_file")

    logger.debug(f"Parameters: x = {x}, y = {y}")
    logger.info(f"Module {module_name} started")
    logger.warning(f"Step {step_name} encountered null values")
    logger.error("Something bad happened")

    # Use error to raise an error yourself
    if something_very_bad:
        logger.error("Something very bad happened", exception_type=TypeError)

    # Use within try/except block when we want to continue
    try:
        load_parameters()
    except FileNotFoundError:
        logger.error("Could not find file; creating it instead", reraise=False)
        create_file()

    # Use within try/except and raise the exception
    try:
        load_parameters()
    except FileNotFoundError:
        logger.error("Could not find file; please create it", reraise=True)


Configure a file sink with ``set_file_sink``, and then use the other methods to log at
various levels. By default, logs at the INFO level or higher are sent to the console,
and logs at the DEBUG level are only sent to log files (if configured).

"""

import logging
import sys
from pathlib import Path
from datetime import datetime


# Set up root logger at lowest logging level; if this were logging.INFO then log
# statements at lower levels (DEBUG) would be ignored.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Set up default format for log messages
base_formatter = logging.Formatter(
    fmt='%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S'
)

# Configure stream logging at INFO level by default, but only if there is not already
# a StreamHandler (otherwise messages will be duplicated)
if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(base_formatter)
    logger.addHandler(stream_handler)


def set_file_sink(name: str):
    """Set up file sink with provided name and current timestamp."""
    if not Path('logs/').exists():
        Path('logs/').mkdir()

    timestamp = datetime.now().strftime(f'%Y_%m_%d_%H%M%S')

    # Create a new FileHandler and configure it separately
    file_handler = logging.FileHandler(filename=f'logs/{name}_{timestamp}.log')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(base_formatter)
    logger.addHandler(file_handler)


def debug(message: str):
    """Log a message at the DEBUG level.

    The DEBUG level should be used for information that is useful when diagnosing
    issues. This is more detailed than INFO, so examples might include values of
    parameters, code paths taken, or other detailed information.

    Note that debug messages are not logged to the console; they will only appear if
    you set up a file sink with ``set_file_sink``. A future release will make this
    behavior configurable.

    Parameters
    ----------
    message: str
        The message to be logged.

    """
    logging.log(logging.DEBUG, message)


def info(message: str):
    """Log a message at the INFO level.

    The INFO level should be used for events occurring during normal operation of a
    program, e.g., for status monitoring. Use the INFO level to inform readers of what
    is happening at a high level.

    Parameters
    ----------
    message: str
        The message to be logged.

    """
    logging.log(logging.INFO, message)


def warning(message: str):
    """Log a message at the WARNING level.

    The WARNING level should be used to issue warnings about runtime events; this is
    appropriate for events that users should investigate into more detail. Examples
    may include data issues that users have little control over, something unexpected
    happening, or something that may turn into a problem in the future.

    Parameters
    ----------
    message: str
        The message to be logged.

    """
    logging.log(logging.WARNING, message)


def error(message: str, exception_type: type = None, reraise: bool = False):
    """Log a message at the ERROR level.

    The ERROR level should be used for errors that the program cannot (or should not)
    overcome. This should typically be used in one of two places: (1) to log an error
    while also raising it to the caller, or (2) to catch an error in the ``except``
    block of a try/except construct, optionally reraising the error.

    Examples
    --------
    .. code:: python

        # Log an error without doing anything else; not recommended and this should
        # almost certainly be a warning
        logger.error("Something bad happened")

        # Use to raise an error yourself
        if something_very_bad:
            logger.error("Something very bad happened", exception_type=TypeError)

        # Use within try/except block when we want to continue
        try:
            load_parameters()
        except FileNotFoundError:
            logger.error("Could not find file; creating it instead", reraise=False)
            create_file()

        # Use within try/except and raise the exception
        try:
            load_parameters()
        except FileNotFoundError:
            logger.error("Could not find file; please create it", reraise=True)

    Parameters
    ----------
    message: str
        The message to be logged.

    exception_type: type
        The type of error to log, e.g., ``ValueError``, ``TypeError``, etc. If this is
        set, then the stack frame gets logged at the DEBUG level to ensure the
        exception info gets captured.

    reraise: bool, optional
        When used in a try/except block, whether or not to raise the exception that
        occurred. Keep this as False if the error will be handled, but set it to True
        if the code should stop.

        When used outside try/except, this parameter has no effect.

    """
    # If exception_type was specified, capture stack frame then raise it
    if exception_type:
        logging.log(logging.ERROR, message)
        logging.log(logging.DEBUG, "Stack frame of previous error: ", stack_info=True)
        raise exception_type(message)

    # Determine if there is an active exception
    is_active_exception = sys.exc_info() != (None, None, None)

    # Log the error, capturing exception info if it exists
    logging.log(logging.ERROR, message, exc_info=is_active_exception)

    # Reraise if the user asks and if there is an active exception. We have to check
    # for an active exception, because using reraise=True without an active exception
    # will itself cause an exception ... so we stop users from shooting themselves in
    # the foot by doing that.
    if reraise and is_active_exception:
        raise
