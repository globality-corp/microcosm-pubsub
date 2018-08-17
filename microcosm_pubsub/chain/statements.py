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


class ForEachStatement:
    def __init__(self, key):
        self.key = key
        self.items = None
        self.chain = None

    def __str__(self):
        return f"for_{self.key}"

    def in_(self, items):
        self.items = items
        return self

    def do(self, chain):
        self.chain = chain
        return self

    def __call__(self, context):
        # Validate to ensure the key is not used in the context
        # as we use assign in the loop
        context[self.key] = None

        responses = []
        for item in context[self.items]:
            context.assign(self.key, item)
            responses.append(self.chain(context))

        # Set the responses in the context
        context[f"{self.key}_list"] = responses

        return responses


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


def for_each(key):
    """
    Run the chain for all items in array
    Each items will be accessible as "item" name

    Example: for("item")
        .in_(items)
        .do(Chain(...))

    """
    return ForEachStatement(key)
