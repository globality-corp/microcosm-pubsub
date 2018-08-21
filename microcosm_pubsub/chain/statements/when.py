"""
when("foo").then(
    ...
).otherwise(
    ...
)

"""
from microcosm_pubsub.chain import Chain


class WhenStatement:
    """
    Condition chain control on a key.

    """
    def __init__(self, key):
        self.key = key
        self._then = None
        self._otherwise = None

    def then(self, *args, **kwargs):
        self._then = Chain.make(*args, **kwargs)
        return self

    def otherwise(self, *args, **kwargs):
        self._otherwise = Chain.make(*args, **kwargs)
        return self

    def __str__(self):
        return f"when_{self.key}"

    def __call__(self, context):
        if context[self.key]:
            if self._then:
                return self._then(context)
        elif self._otherwise:
            return self._otherwise(context)


def when(key):
    """
    Run one of two chains - based on a condition

    Example: when("arg")
        .then(Chain(...))
        .otherwise(Chain(...))

    """

    return WhenStatement(key)
