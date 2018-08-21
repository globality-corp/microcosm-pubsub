"""
try(
    ...
).catch("foo").then(
    ...
).catch("bar").then(
    ...
).otherwise(
    ...
)

"""
from microcosm_pubsub.chain import Chain
from microcosm_pubsub.chain.statements.case import CaseStatement


class TryChainStatement:
    def __init__(self, *args, **kwargs):
        self.chain = Chain.make(*args, **kwargs)
        self._cases = dict()
        self._otherwise = None

    def catch(self, key, *args,  **kwargs):
        if not args and not kwargs:
            return CaseStatement(self, key)

        self._cases[key] = Chain.make(*args, **kwargs)
        return self

    def otherwise(self, *args, **kwargs):
        self._otherwise = Chain.make(*args, **kwargs)
        return self

    def __call__(self, context):
        try:
            res = self.chain(context)
        except Exception as error:
            handle = self._cases.get(type(error))
            if not handle:
                raise error
            return handle(context)
        else:
            if self._otherwise:
                return self._otherwise(context)
            return res


def try_chain(*args, **kwargs):
    """
    Run one of two chains - based on a condition

    Example: try_chain(Chain(...))
        .catch(ValueError, Chain(...))
        .catch(KeyError, Chain(...))
        .otherwise(Chain(...))

    """
    return TryChainStatement(*args, **kwargs)
