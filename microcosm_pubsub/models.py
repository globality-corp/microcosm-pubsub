"""
Microcosm Pubsub Models

"""


class SNSIntrospection(object):
    def __init__(
        self,
        media_type,
        route,
        call_module,
        call_function,
    ):
        self.media_type = media_type
        self.route = route
        self.call_module = call_module
        self.call_function = call_function

    def __hash__(self):
        return hash((self.media_type, self.route, self.call_module, self.call_function))

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return all((
            self.media_type == other.media_type,
            self.route == other.route,
            self.call_module == other.call_module,
            self.call_function == other.call_function,
        ))
