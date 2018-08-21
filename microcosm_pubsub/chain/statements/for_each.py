"""
for_each("item").in_("items").do(...)

"""
from microcosm_pubsub.chain import Chain


class ForEachStatement:
    def __init__(self, key):
        self.key = key
        self.items = None
        self.chain = None

    def __str__(self):
        return f"for_{self.key}"

    @property
    def list_key(self):
        return f"{self.key}_list"

    def in_(self, items):
        self.items = items
        return self

    def do(self, *args, **kwargs):
        self.chain = Chain.make(*args, **kwargs)
        return self

    def __call__(self, context):
        responses = [
            self.chain(context.local(**{
                self.key: item,
            }))
            for item in context[self.items]
        ]

        # Set the responses in the context
        context[self.list_key] = responses

        return responses


def for_each(key):
    """
    Run the chain for all items in array
    Each items will be accessible as "item" name

    Example: for("item")
        .in_(items)
        .do(Chain(...))

    """
    return ForEachStatement(key)
