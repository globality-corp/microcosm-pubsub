from microcosm_pubsub.chain import Chain


class ArgumentExtractor:
    """
    Extract an `this` value into a new context value `that`.

    """
    def __init__(self, this, that=None):
        self.parts = this.split(".")
        self.that = that

    def to(self, that):
        self.that = that
        return self

    @property
    def key(self):
        return self.parts[0]

    @property
    def name(self):
        return self.that

    def __str__(self):
        return f"extract_{self.name}"

    def __call__(self, context):
        value = context[self.key]

        for part in self.parts[1:]:
            if hasattr(value, part):
                value = getattr(value, part)
            else:
                value = value[part]

        context[self.name] = value
        return value


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


class CaseStatement:
    def __init__(self, switch, key):
        self.switch = switch
        self.key = key

    def then(self, *args, **kwargs):
        self.switch._cases[self.key] = Chain.make(*args, **kwargs)
        return self.switch


class SwitchStatement:
    """
    Switch on one more cases.

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


class TryStatement:
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


def extract(name, key, key_property=None):
    """
    Extract an argument from a context to anotherwise context key

    :param name: new context key
    :param key: old context key
    :param key_property: propery of the context key

    """
    if key_property:
        key = ".".join([key, key_property])

    return ArgumentExtractor(key, name)


def assign(this):
    return ArgumentExtractor(this)


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


def try_chain(*args, **kwargs):
    """
    Run one of two chains - based on a condition

    Example: try_chain(Chain(...))
        .catch(ValueError, Chain(...))
        .catch(KeyError, Chain(...))
        .otherwise(Chain(...))

    """
    return TryStatement(*args, **kwargs)


def for_each(key):
    """
    Run the chain for all items in array
    Each items will be accessible as "item" name

    Example: for("item")
        .in_(items)
        .do(Chain(...))

    """
    return ForEachStatement(key)
