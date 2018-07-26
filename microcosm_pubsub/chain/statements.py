from marshmallow import ValidationError


class ArgumentExtractor:
    def __init__(self, name, key, key_property=None):
        self.name = name
        self.key = key
        self.key_property = key_property

    def __str__(self):
        return f"extract_{self.name}"

    def __call__(self, context):
        if self.name in context:
            raise ValidationError(f"Variable '{self.name}'' already extracted")

        obj = context[self.key]
        if self.key_property is None:
            value = obj
        elif hasattr(obj, self.key_property):
            value = getattr(obj, self.key_property)
        else:
            value = obj[self.key_property]

        context[self.name] = value
        return value


class IfStatement:
    def __init__(self, name, chain=None, otherwise=None):
        self.name = name
        self.chain = chain
        self.otherwise = otherwise

    def __str__(self):
        return f"when_{self.name}"

    def __call__(self, context):
        if context[self.name]:
            if self.chain:
                return self.chain(context)
        elif self.otherwise:
            return self.otherwise(context)


class SwitchStatement:
    def __init__(self, name, cases, otherwise=None):
        self.name = name
        self.otherwise = otherwise
        self.cases = cases

    def __str__(self):
        return f"switch_{self.name}"

    def __call__(self, context):
        key = context[self.name]
        action = self.cases.get(key, self.otherwise)
        if action:
            return action(context)


class TryStatement:
    def __init__(self, chain, catch, otherwise=None):
        self.chain = chain
        self.catch = catch
        self.otherwise = otherwise

    def __call__(self, context):
        try:
            res = self.chain(context)
        except Exception as excption:
            handle = self.catch.get(type(excption))
            if not handle:
                raise excption
            return handle(context)
        else:
            if self.otherwise:
                return self.otherwise(context)
            return res


def extract(name, key, key_property=None):
    """
    Extract an argument from a context to anotherwise context key

    :param name: new context key
    :param from_: old context key
    :param property_: propery of the context key

    """
    return ArgumentExtractor(name, key, key_property)


def when(name, chain=None, otherwise=None):
    """
    Run one of two chains - based on a condition

    :param name: context key
    :param chain: Chain
    :param otherwise: Chain

    """

    return IfStatement(name, chain, otherwise)


def switch(name, otherwise=None, cases=None, **kwargs):
    """
    Run one of number of chains chains - based on a key value

    :param name: context key
    :param cases: dict / list of tuple - Chains to run based on keys
    :param **kwargs: Chain to run based on string keys
    :param otherwise: Chain to run if no case was matched

    """
    cases_ = kwargs
    if cases:
        cases_.update(cases)
    return SwitchStatement(name, cases_, otherwise)


def try_chain(chain, catch=None, otherwise=None):
    """
    Run one of two chains - based on a condition

    :param chain: Chain to run
    :param catch: dict / list of tuple - Chains to run if an exception was raised
    :param otherwise: Chain to run if no exption was raised

    """
    cases_ = dict(catch) if catch else dict()
    return TryStatement(chain, cases_, otherwise)
