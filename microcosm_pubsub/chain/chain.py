from marshmallow import ValidationError

from microcosm_pubsub.chain.context_decorators import (
    get_from_context,
    save_to_context,
    save_to_context_by_func_name,
    temporarily_replace_context_keys,
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
        self.links = list(args)

    @property
    def context_decorators(self):
        return [
            # Order matter - get_from_context should be first
            get_from_context,
            temporarily_replace_context_keys,
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
                raise ValidationError(f"{key} already in the context")
            context[key] = value

        # Allow self refrence
        context.update(context=context)

        for link in self.links:
            res = self.apply_decorators(context, link)()
        return res

    def apply_decorators(self, context, link):
        decorated_link = link
        for decorator in self.context_decorators:
            decorated_link = decorator(context, decorated_link)
        return decorated_link
