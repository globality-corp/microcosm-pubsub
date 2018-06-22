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
        count,
    ):
        self.media_type = media_type
        self.route = route
        self.call_module = call_module
        self.call_function = call_function
        self.count = count
