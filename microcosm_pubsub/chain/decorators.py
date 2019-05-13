from functools import wraps
from inspect import ismethod


EXTRACTS = "_extracts"
BINDS = "_binds"


def to_function(callable_object):
    """
    In python, we can't use setattr on a method
    We can instead wrap the method in a function.
    This function will wrap only methods.

    """
    if not ismethod(callable_object):
        return callable_object

    @wraps(callable_object)
    def decorated_method(*args, **kwargs):
        return callable_object(*args, **kwargs)
    return decorated_method


def extracts(*extract):
    """
    On resolving this function with get_from_context decorator -
    updates the context based on the function return value.

    Note: The decorator just marks the function and don't change the return value.
    :param *args: key/s of the return value

    """
    def decorate(func):
        function = to_function(func)
        setattr(function, EXTRACTS, extract)
        return function
    return decorate


def binds(**binds):
    """
    On resolving this function with temporarily_replace_context_keys decorator -
    temporarily updates the context keys.

    Note: The decorator just marks the function.
    :param **binds: (old_context_key, new_context_key) - context keys to rename

    """
    def decorate(func):
        function = to_function(func)
        setattr(function, BINDS, binds)
        return function
    return decorate
