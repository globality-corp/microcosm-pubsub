EXTRACTS = "_extracts"
BINDS = "_binds"


def extracts(*extract):
    """
    On resolving this function with get_from_context decorator -
    updates the context based on the function return value.

    Note: The decorator just marks the function and don't change the return value.
    :param *args: key/s of the return value

    """
    def decorate(func):
        setattr(func, EXTRACTS, extract)
        return func
    return decorate


def binds(**binds):
    """
    On resolving this function with temporarily_replace_context_keys decorator -
    temporarily updates the context keys.

    Note: The decorator just marks the function.
    :param **binds: (old_context_key, new_context_key) - context keys to rename

    """
    def decorate(func):
        setattr(func, BINDS, binds)
        return func
    return decorate
