"""
switch("foo").case("bar").then(
    ...
).case("baz").then(
    ...
).otherwise(
    ...
)

"""
from microcosm_pubsub.chain import Chain
from microcosm_pubsub.chain.statements.case import CaseStatement


class SwitchStatement:
    """
    Switch on one or more cases.

    """
    def __init__(self, key):
        self.key = key
        self._otherwise = None
        self._cases = dict()

    def __str__(self):
        return f"switch_{self.key}"

    def case(self, key, *args, **kwargs):
        if not args and not kwargs:
            return CaseStatement(self, key)

        self._cases[key] = Chain.make(*args, **kwargs)
        return self

    def otherwise(self, *args, **kwargs):
        self._otherwise = Chain.make(*args, **kwargs)
        return self

    def __call__(self, context):
        key = context[self.key]
        action = self._cases.get(key, self._otherwise)
        if action:
            return action(context)


def switch(key):
    """
    Run one of number of chains chains - based on a key value
    Example: when("arg")
        .case(100, Chain(...))
        .case(200, Chain(...))
        .otherwise(Chain(...))

    """
    return SwitchStatement(key)
