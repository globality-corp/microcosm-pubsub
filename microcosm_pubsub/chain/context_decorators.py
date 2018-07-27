from functools import wraps
from inspect import getfullargspec, ismethod, isfunction

from microcosm_pubsub.chain.decorators import EXTRACTS, BINDS


EXTRACTS_PREFIX = "extracts_"


def get_from_context(context, func):
    """
    Decorate a function - pass to the function the relevant arguments
    from a context (dictionary) - based on the function arg names

    """
    include_self = ismethod(func) or not isfunction(func)
    args_names = getfullargspec(func)[0]

    @wraps(func)
    def decorate(*args, **kwargs):
        context_kwargs = {
            arg_name: context[arg_name]
            for arg_name in args_names[int(include_self) + len(args):]
            if arg_name not in kwargs
        }
        return func(*args, **kwargs, **context_kwargs)
    return decorate


def save_to_context(context, func):
    """
    Decorate a function - save to a context (dictionary) the function return value
    if the function is marked by @extracts decorator

    """
    extracts = getattr(func, EXTRACTS, None)
    if not extracts:
        return func
    extracts_one_value = len(extracts) == 1

    @wraps(func)
    def decorate(*args, **kwargs):
        value = func(*args, **kwargs)
        if extracts_one_value:
            value = [value]
        for index, name in enumerate(extracts):
            context[name] = value[index]
        return value
    return decorate


def save_to_context_by_func_name(context, func):
    """
    Decorate a function - save to a context (dictionary) the function return value
    if the function is not signed by EXTRACTS and it's name starts with "extracts_"

    """
    if (
        hasattr(func, EXTRACTS) or
        not hasattr(func, "__name__") or
        not func.__name__.startswith(EXTRACTS_PREFIX)
    ):
        return func
    name = func.__name__[len(EXTRACTS_PREFIX):]

    @wraps(func)
    def decorate(*args, **kwargs):
        value = func(*args, **kwargs)
        context[name] = value
        return value
    return decorate


def temporarily_replace_context_keys(context, func):
    """
    Decorate a function - temporarily updates the context keys while running the function
    Updates the context if the function is marked by @binds decorator.

    """
    binds = getattr(func, BINDS, None)
    if not binds:
        return func

    @wraps(func)
    def decorate(*args, **kwargs):
        for old_key, new_key in binds.items():
            if old_key not in context:
                raise KeyError(f"Variable '{old_key}'' not set")
            if new_key in context:
                raise ValueError(f"Variable '{new_key}'' already set")
        try:
            for old_key, new_key in binds.items():
                context[new_key] = context.pop(old_key)
            return func(*args, **kwargs)
        finally:
            for old_key, new_key in binds.items():
                context[old_key] = context.pop(new_key)
    return decorate
