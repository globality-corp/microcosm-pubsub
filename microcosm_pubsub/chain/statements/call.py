"""
Syntactic sugar using existing primitives defined

"""
from microcosm_pubsub.chain import Chain
from microcosm_pubsub.chain.context import ScopedSafeContext


class LocalCallWrapper:
    """
    Syntactic sugar making it easy to call the same function multiple times in a chain
    Usage:
    Chain(
       ...,
       call(a_function).with_args(a_function_arg="an_existing_context_key").as_("name_of_the_result_in_context"),
       call(a_function).with_args(a_function_arg="some_other_key").as_("another_name"),
    )

    """
    def __init__(self, func):
        self.func = func
        self.chain = Chain(func)
        self.kwargs = dict()
        self.result_name = None
        self.local_context = None

    def with_args(self, **kwargs):
        self.kwargs = kwargs
        return self

    def as_(self, name):
        self.result_name = name
        return self

    def _build_local_kwargs(self, parent_context):
        local_kwargs = dict()

        for local_name, mapped_value in self.kwargs.items():
            if local_name in parent_context:
                raise ValueError(f"Argument `{local_name}` for local call overshadows existing context key")

            if isinstance(mapped_value, str) and mapped_value in parent_context:
                # mapped value points to parent context
                local_kwargs[local_name] = parent_context[mapped_value]
            else:
                # mapped value is a constant
                local_kwargs[local_name] = mapped_value

        return local_kwargs

    def __call__(self, context):
        local_kwargs = self._build_local_kwargs(context)
        self.local_context = ScopedSafeContext(context, **local_kwargs)
        result = self.chain(self.local_context)
        context[self.result_name] = result
        return result


def call(func):
    return LocalCallWrapper(func)
