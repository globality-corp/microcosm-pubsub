"""
assign("foo.bar").to("baz")

"""


class AssignStatement:
    """
    Assign `this` value as `that`.

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
        return f"assign_{self.name}"

    def __call__(self, context):
        value = context[self.key]

        for part in self.parts[1:]:
            if hasattr(value, part):
                value = getattr(value, part)
            else:
                value = value[part]

        context[self.name] = value
        return value


def assign(this):
    return AssignStatement(this)


def extract(name, key, key_property=None):
    """
    Extract an argument from a context to anotherwise context key

    :param name: new context key
    :param key: old context key
    :param key_property: propery of the context key

    """
    if key_property:
        key = ".".join([key, key_property])

    return AssignStatement(key, name)
