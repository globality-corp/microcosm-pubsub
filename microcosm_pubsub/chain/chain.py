from marshmallow import ValidationError

from microcosm_pubsub.chain.context_decorators import (
    get_from_context,
    save_to_context,
    save_to_context_by_func_name,
    temporary_replace_context_keys,
)


class Chain:
    """
    Structure that contains callable functions that shares the same context.

    """
    def __init__(self, *args):
        """
        Build a chain that can be resolved multiple times
        :param *args: callable functions, Chain statemnts or other Chains

        """
        self.pieces = args

        self.context_decorators = [
            # Order matter - get_from_context should be first
            get_from_context,
            temporary_replace_context_keys,
            save_to_context,
            save_to_context_by_func_name,
        ]

    def __call__(self, context=None):
        """
        Resolve the chain and return the last chain function result
        :param context: optional argument - dictionary to share the context

        """
        return self.resolve(context=context)

    def resolve(self, context=None, **kwargs):
        """
        Resolve the chain and return the last chain function result
        :param context: optional argument - dictionary to share the context
        :param **kwargs: initialize the context with some values

        """
        context = context or dict()

        for key, value in kwargs.items():
            if key in context:
                raise ValidationError(f"{key} alredy in the context")
            context[key] = value

        # Allow self refrence
        context["context"] = context

        for piece in self.pieces:
            res = self.apply_decorators(context, piece)()
        return res

    def apply_decorators(self, context, func):
        decorated_func = func
        for decorator in self.context_decorators:
            decorated_func = decorator(context, decorated_func)
        return decorated_func
