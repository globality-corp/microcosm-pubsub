"""
PubSub errors.

"""


class TopicNotDefinedError(Exception):
    pass


class Nack(Exception):
    """
    An error that causes the current message to be nacked.

    """
    def __init__(self, visibility_timeout_seconds):
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


class TTLExpired(Exception):
    """
    Control-flow exception to skip messages in infinte loop.

    """
    def __init__(self, reason=None, extra=None):
        super().__init__(reason)
        self.extra = extra or dict()
