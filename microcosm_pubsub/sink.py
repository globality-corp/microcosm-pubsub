"""
Process messages through to a "sink" function.

"""
from collections import namedtuple
from logging import getLogger


SinkResult = namedtuple("SinkResult", ["num_messages", "num_errors", "num_ignored"])


logger = getLogger("pubsub.sink")


def sink_to(consumer, func):
    """
    Send a batch of messages to a function.

    :param consumer: a consumer
    :param func: a function that takes message content and returns a truthy value
    :returns: a `SinkResult` with metrics around message consumption

    """
    num_messages = num_errors = num_ignored = 0
    for message in consumer.consume():
        num_messages += 1
        try:
            with message:
                if not func(message.content):
                    num_ignored += 1
        except Exception:
            logger.info("Error handling SQS message.", exc_info=True)
            num_errors += 1

    return SinkResult(num_messages, num_errors, num_ignored)
