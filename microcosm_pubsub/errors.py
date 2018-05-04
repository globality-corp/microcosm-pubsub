"""
PubSub errors.

"""


class TopicNotDefinedError(Exception):
    pass


class Nack(Exception):
    """
    An error that causes the current message to be nacked.

    """
    def __init__(self,  visibility_timeout_seconds, reason=None, extra=None):
        super().__init__(reason)
        self.extra = extra or dict()
        self.visibility_timeout_seconds = visibility_timeout_seconds

    def __repr__(self):
        return "Nack({})".format(self.visibility_timeout_seconds)


class SkipMessage(Exception):
    """
    Control-flow exception to skip resource processing.

    """
    def __init__(self, reason, extra=None):
        super().__init__(reason)
        self.extra = extra or dict()
