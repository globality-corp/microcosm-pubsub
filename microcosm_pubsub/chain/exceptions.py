class AttributeNotFound(Exception):
    def __init__(self, parent, attribute):
        super().__init__(f"Failed to find attribute `{attribute}` on `{parent}`")


class ContextKeyNotFound(Exception):
    def __init__(self, key_error: KeyError, func):
        context_key, *_ = key_error.args
        super().__init__(f"Failed to find context key `{context_key}` during evaluation of `{func}`")
