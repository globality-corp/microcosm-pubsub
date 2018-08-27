"""
assign("foo.bar").to("baz")

"""
from inspect import getfullargspec


class Reference:

    def __init__(self, name):
        self.parts = name.split(".")

    @property
    def key(self):
        return self.parts[0]

    def __call__(self, context):
        value = context[self.key]

        for part in self.parts[1:]:
            if hasattr(value, part):
                value = getattr(value, part)
            else:
                value = value[part]

        return value


class Constant:

    def __init__(self, value):
        self.value = value

    def __call__(self, context):
        return self.value


class Function:

    def __init__(self, func):
        self.func = func
        self.argspec = getfullargspec(func)

    def __call__(self, context):
        if self.argspec.args:
            return self.func(context)
        else:
            return self.func()


class AssignStatement:
    """
    Assign `this` value as `that`.

    """
    def __init__(self, this, that=None):
        self.this = this
        self.that = that

    def to(self, that):
        self.that = that
        return self

    @property
    def name(self):
        return self.that

    def __str__(self):
        return f"assign_{self.name}"

    def __call__(self, context):
        value = self.this(context)
        context[self.name] = value
        return value


def assign(this):
    return AssignStatement(Reference(this))


def assign_constant(this):
    return AssignStatement(Constant(this))


def assign_function(this):
    return AssignStatement(Function(this))


def extract(name, key, key_property=None):
    """
    Extract an argument from a context to anotherwise context key

    :param name: new context key
    :param key: old context key
    :param key_property: propery of the context key

    """
    if key_property:
        key = ".".join([key, key_property])

    return AssignStatement(Reference(key), name)
