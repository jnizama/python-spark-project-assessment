"""
decorators.py
Reusable decorators used across the application.
"""
import logging
import time
from functools import wraps

logger = logging.getLogger(__name__)

def log_execution(func):
    """
    Decorator that logs the execution time of a function.

    Parameters
    ----------
    func
        Function to decorate.

    Returns
    -------
    callable
        Wrapped function.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.info("Starting: %s", func.__name__)
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        logger.info(
            "Finished: %s (%.3f seconds)",
            func.__name__,
            elapsed
        )
        return result
    return wrapper