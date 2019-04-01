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
from microcosm_pubsub.chain.statements.switch import SwitchStatement


class TryChainStatement(SwitchStatement):
    def __init__(self, *args, **kwargs):
        self.chain = Chain.make(*args, **kwargs)
        super().__init__(self.chain)

    def catch(self, key, *args,  **kwargs):
        return self.case(key, *args, **kwargs)

    def __call__(self, context):
        try:
            res = self.chain(context)
        except Exception as error:
            handle = self._case_for_key(type(error))
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
