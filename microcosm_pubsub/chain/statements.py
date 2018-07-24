from marshmallow import ValidationError


class ArgumentExtractor:
    def __init__(self, name, from_, property_=None):
        self.name = name
        self.from_ = from_
        self.property_ = property_

    def __str__(self):
        return f"extract_{self.name}"

    def __call__(self, context):
        if self.name in context:
            raise ValidationError(f"Variable '{self.name}'' alredy extracted")

        obj = context[self.from_]
        if self.property_ is None:
            value = obj
        elif hasattr(obj, self.property_):
            value = getattr(obj, self.property_)
        else:
            value = obj[self.property_]

        context[self.name] = value
        return value


class IfStatement:
    def __init__(self, name, then_=None, else_=None):
        self.name = name
        self.then_ = then_
        self.else_ = else_

    def __str__(self):
        return f"if_{self.name}"

    def __call__(self, context):
        if context[self.name]:
            if self.then_:
                return self.then_(context)
        elif self.else_:
            return self.else_(context)


class SwitchStatement:
    def __init__(self, name, cases, else_=None):
        self.name = name
        self.else_ = else_
        self.cases = cases

    def __str__(self):
        return f"switch_{self.name}"

    def __call__(self, context):
        key = context[self.name]
        action = self.cases.get(key, self.else_)
        if action:
            return action(context)


class TryStatement:
    def __init__(self, try_, except_, else_=None):
        self.try_ = try_
        self.except_ = except_
        self.else_ = else_

    def __call__(self, context):
        try:
            res = self.try_(context)
        except Exception as excption:
            handle = self.except_.get(type(excption))
            if not handle:
                raise excption
            return handle(context)
        else:
            if self.else_:
                return self.else_(context)
            return res


def extract_(name, from_, property_=None):
    return ArgumentExtractor(name, from_, property_)


def if_(name, then_=None, else_=None):
    return IfStatement(name, then_, else_)


def switch_(name, else_=None, cases=None, **kwargs):
    cases_ = kwargs
    if cases:
        cases_.update(cases)
    return SwitchStatement(name, cases_, else_)


def try_(try_, except_=None, else_=None):
    cases_ = dict(except_) if except_ else dict()
    return TryStatement(try_, cases_, else_)
