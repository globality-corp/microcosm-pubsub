from marshmallow import ValidationError

from microcosm_pubsub.chain.context_decorators import (
    get_from_context,
    save_to_context,
    save_to_context_by_func_name,
    temporary_replace_context_keys,
)


class Chain:
    def __init__(self, *args):
        self.pieces = args

        self.context_decorators = [
            # Order matter
            get_from_context,
            temporary_replace_context_keys,
            save_to_context,
            save_to_context_by_func_name,
        ]

    def __call__(self, context, **kwargs):
        context["context"] = context
        for piece in self.pieces:
            res = self.apply_decorators(context, piece)()
        return res

    def resolve(self, context=None, **kwargs):
        context = context or dict()
        for key, value in kwargs.items():
            if key in context:
                raise ValidationError(f"{key} alredy in the context")
            context[key] = value
        return self.__call__(context)

    def apply_decorators(self, context, func):
        decorated_func = func
        for decorator in self.context_decorators:
            decorated_func = decorator(context, decorated_func)
        return decorated_func
