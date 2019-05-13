from microcosm_pubsub.chain.context import SafeContext
from microcosm_pubsub.chain.context_decorators import (
    DEFAULT_ASSIGNED,
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
        """
        Decorators to apply to the chain links

        """
        return [
            # Order matter - get_from_context should be first
            get_from_context,
            temporarily_replace_context_keys,
            save_to_context,
            save_to_context_by_func_name,
        ]

    @property
    def context_decorators_assigned(self):
        """
        Tuple naming attributes that self.context_decorators attributes should keep
        It should contain all attributes that set by relevant decorators
        (such as @extracts)

        """
        return DEFAULT_ASSIGNED

    @property
    def new_context_type(self):
        """
        Context type to create

        """
        return SafeContext

    def __call__(self, context=None, **kwargs):
        """
        Resolve the chain and return the last chain function result
        :param context: use existing context instead of creating a new one
        :param **kwargs: initialize the context with some values

        """
        context = context or self.new_context_type()
        context.update(kwargs)

        for link in self.links:
            func = self.apply_decorators(context, link)
            res = func()

        return res

    def __len__(self):
        return len(self.links)

    def apply_decorators(self, context, link):
        decorated_link = link
        for decorator in self.context_decorators:
            decorated_link = decorator(context, decorated_link, self.context_decorators_assigned)
        return decorated_link

    @classmethod
    def make(cls, *args, chain=None, links=None, **kwargs):
        if chain is not None:
            return chain
        elif links is not None:
            return cls(*links)
        elif args:
            return cls(*args)
        else:
            raise ValueError("Must define one of 'chain', 'links', or '*args'")
