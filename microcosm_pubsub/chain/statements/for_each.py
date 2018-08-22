"""
for_each("item").in_("items").do(...)

"""
from microcosm_pubsub.chain import Chain


def yes(value):
    return True


def not_none(value):
    return value is not None


class ForEachStatement:
    def __init__(self, key, list_key=None):
        self.key = key
        self.items = None
        self.chain = None
        self.list_key = list_key or f"{self.key}_list"
        self.filter_func = yes

    def __str__(self):
        return f"for_{self.key}"

    def in_(self, items):
        self.items = items
        return self

    def as_(self, list_key):
        self.list_key = list_key
        return self

    def when(self, filter_func):
        self.filter_func = filter_func
        return self

    def when_not_none(self):
        return self.when(not_none)

    def do(self, *args, **kwargs):
        self.chain = Chain.make(*args, **kwargs)
        return self

    def __call__(self, context):
        values = [
            self.chain(context.local(**{
                self.key: item,
            }))
            for item in context[self.items]
        ]

        filtered_values = list(filter(self.filter_func, values))

        # Set the responses in the context
        context[self.list_key] = filtered_values
        return filtered_values


def for_each(key):
    """
    Run the chain for all items in array
    Each items will be accessible as "item" name

    Example: for("item")
        .in_(items)
        .do(Chain(...))

    """
    return ForEachStatement(key)
