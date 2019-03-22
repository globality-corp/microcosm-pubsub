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
        self._cases = []

    def __str__(self):
        return f"switch_{self.key}"

    def _add_action_for_key(self, key, action):
        self._cases.append((key, action))

    def _case_for_key(self, key):
        try:
            return next(
                case_action
                for case_key, case_action in self._cases
                if case_key == key
            )
        except StopIteration:
            return None

    def case(self, key, *args, **kwargs):
        if not args and not kwargs:
            return CaseStatement(self, key)

        self._cases.append(
            (key, Chain.make(*args, **kwargs)),
        )
        return self

    def otherwise(self, *args, **kwargs):
        self._otherwise = Chain.make(*args, **kwargs)
        return self

    def __call__(self, context):
        key = context[self.key]
        action = self._case_for_key(key)
        if action is None:
            action = self._otherwise
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
