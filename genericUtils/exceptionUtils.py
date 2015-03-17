#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"

from functools import wraps
import signal
import os
import errno


class TimeOutError(Exception):
    pass


def timeout(seconds=10, errormsg=os.strerror(errno.ETIME)):
    """
    Decorator function to cuase timeout after specified time

    """
    def decorator(func):
        def _timeoutHandler(signum, errormsg):
            raise TimeOutError(errormsg)

        @wraps(func)
        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _timeoutHandler)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)

            return result
        return wrapper
    return decorator
