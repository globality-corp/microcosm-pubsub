"""
PubSub control flow.

"""


class TopicNotDefinedError(Exception):
    """
    No topic was configured for message media type.

    """
    pass


class Nack(Exception):
    """
    Retry the message after a configured number of seconds.

    Note that retry behavior is contingent on both the backoff policy
    and the SQS dead letter queue (DLQ) configuration, if any.

    """
    def __init__(self, visibility_timeout_seconds):
        self.visibility_timeout_seconds = visibility_timeout_seconds

    def __repr__(self):
        return "Nack({})".format(self.visibility_timeout_seconds)


class SkipMessage(Exception):
    """
    Stop processing a message and do not retry processing.

    Indicates that message content failed a precondition within a handler.

    """
    def __init__(self, reason, extra=None):
        super().__init__(reason)
        self.extra = extra or dict()


class IgnoreMessage(Exception):
    """
    Do not processing a message and do not count it for statistics.

    Indicates that message was not processable by the daemon.

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
