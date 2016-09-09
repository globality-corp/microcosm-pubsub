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
