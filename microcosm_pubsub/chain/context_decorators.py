from functools import wraps
from marshmallow import ValidationError
from inspect import getfullargspec, ismethod, isfunction

from microcosm_pubsub.chain.decorators import EXTRACTS, BINDS


def get_from_context(context, func):
    """
    Decorate a function - pass to the function the relevant arguments
    from a context (dictionary) - based on the function arg names

    """
    include_self = int(ismethod(func) or not isfunction(func))
    args_names = getfullargspec(func)[0]

    @wraps(func)
    def decorate(*passed_args, **passed_kwargs):
        context_kwargs = {
            arg_name: context[arg_name]
            for arg_name in args_names[include_self + len(passed_args):]
            if arg_name not in passed_kwargs
        }
        return func(*passed_args, **passed_kwargs, **context_kwargs)
    return decorate


def save_to_context(context, func):
    """
    Decorate a function - save to a context (dictionary) the function return value
    if the function is marked by @extracts decorator

    """
    extracts = getattr(func, EXTRACTS, None)
    if not extracts:
        return func

    @wraps(func)
    def decorate(*args, **kwargs):
        value = func(*args, **kwargs)
        if len(extracts) == 1:
            value = [value]
        for index, name in enumerate(extracts):
            if name in context:
                raise ValidationError(f"Variable '{name}'' alredy extracted")
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
        not func.__name__.startswith("extracts_")
    ):
        return func
    name = func.__name__[9:]

    @wraps(func)
    def decorate(*args, **kwargs):
        value = func(*args, **kwargs)
        if name in context:
            raise ValidationError(f"Variable '{name}'' alredy extracted")
        context[name] = value
        return value
    return decorate


def temporary_replace_context_keys(context, func):
    """
    Decorate a function - temporary updates the context keys while running the function
    Updates the context if the function is marked by @binds decorator.

    """
    binds = getattr(func, BINDS, None)
    if not binds:
        return func

    @wraps(func)
    def decorate(*args, **kwargs):
        for old_key, new_key in binds.items():
            if old_key not in context:
                raise ValidationError(f"Variable '{old_key}'' not set")
            if new_key in context:
                raise ValidationError(f"Variable '{new_key}'' alredy set")
        try:
            for old_key, new_key in binds.items():
                context[new_key] = context[old_key]
                del context[old_key]
            return func(*args, **kwargs)
        finally:
            for old_key, new_key in binds.items():
                context[old_key] = context[new_key]
                del context[new_key]
    return decorate
