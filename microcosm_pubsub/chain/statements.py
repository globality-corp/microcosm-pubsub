class ArgumentExtractor:
    def __init__(self, name, key, key_property=None):
        self.name = name
        self.key = key
        self.key_property = key_property

    def __str__(self):
        return f"extract_{self.name}"

    def __call__(self, context):
        obj = context[self.key]
        if self.key_property is None:
            value = obj
        elif hasattr(obj, self.key_property):
            value = getattr(obj, self.key_property)
        else:
            value = obj[self.key_property]

        context[self.name] = value
        return value


class WhenStatement:
    def __init__(self, key):
        self.key = key
        self._then = None
        self._otherwise = None

    def then(self, chain):
        self._then = chain
        return self

    def otherwise(self, chain):
        self._otherwise = chain
        return self

    def __str__(self):
        return f"when_{self.key}"

    def __call__(self, context):
        if context[self.key]:
            if self._then:
                return self._then(context)
        elif self._otherwise:
            return self._otherwise(context)


class SwitchStatement:
    def __init__(self, key):
        self.key = key
        self._otherwise = None
        self._cases = dict()

    def __str__(self):
        return f"switch_{self.key}"

    def case(self, key, chain):
        self._cases[key] = chain
        return self

    def otherwise(self, chain):
        self._otherwise = chain
        return self

    def __call__(self, context):
        key = context[self.key]
        action = self._cases.get(key, self._otherwise)
        if action:
            return action(context)


class TryStatement:
    def __init__(self, chain):
        self.chain = chain
        self._cases = dict()
        self._otherwise = None

    def catch(self, key, chain):
        self._cases[key] = chain
        return self

    def otherwise(self, chain):
        self._otherwise = chain
        return self

    def __call__(self, context):
        try:
            res = self.chain(context)
        except Exception as excption:
            handle = self._cases.get(type(excption))
            if not handle:
                raise excption
            return handle(context)
        else:
            if self._otherwise:
                return self._otherwise(context)
            return res


def extract(name, key, key_property=None):
    """
    Extract an argument from a context to anotherwise context key

    :param name: new context key
    :param from_: old context key
    :param property_: propery of the context key

    """
    return ArgumentExtractor(name, key, key_property)


def when(key):
    """
    Run one of two chains - based on a condition

    Example: when("arg")
        .then(Chain(...))
        .otherwise(Chain(...))

    """

    return WhenStatement(key)


def switch(key):
    """
    Run one of number of chains chains - based on a key value
    Example: when("arg")
        .case(100, Chain(...))
        .case(200, Chain(...))
        .otherwise(Chain(...))

    """
    return SwitchStatement(key)


def try_chain(chain):
    """
    Run one of two chains - based on a condition

    Example: try_chain(Chain(...))
        .catch(ValueError, Chain(...))
        .catch(KeyError, Chain(...))
        .otherwise(Chain(...))

    """
    return TryStatement(chain)
