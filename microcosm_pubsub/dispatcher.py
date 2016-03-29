"""
Process batches of messages.

"""
from collections import namedtuple
from logging import getLogger

from microcosm.api import defaults


DispatchResult = namedtuple("DispatchResult", ["message_count", "error_count", "ignore_count"])


logger = getLogger("pubsub.sink")


class SQSMessageDispatcher(object):
    """
    Dispatch batches of SQSMessages to handler functions.

    """
    def __init__(self, sqs_consumer, sqs_message_handlers):
        self.sqs_consumer = sqs_consumer
        self.sqs_message_handlers = sqs_message_handlers

    def handle_batch(self):
        """
        Send a batch of messages to a function.

        :returns: a `DispatchResult` with metrics around message consumption
        """
        message_count = error_count = ignore_count = 0
        for message in self.sqs_consumer.consume():
            message_count += 1
            try:
                with message:
                    if not self.handle_message(message.content):
                        ignore_count += 1
            except Exception:
                logger.info("Error handling SQS message.", exc_info=True)
                error_count += 1

        return DispatchResult(message_count, error_count, ignore_count)

    def handle_message(self, message):
        """
        Handle a single message.

        """
        media_type = message["media_type"]
        sqs_message_handler = self.sqs_message_handlers.get(media_type)
        if sqs_message_handler is None:
            logger.info("Skipping message with unsupported type: {}".format(media_type))
            return False
        return sqs_message_handler(message)


@defaults(
    mappings=dict(),
)
def configure_sqs_message_dispatcher(graph):
    return SQSMessageDispatcher(
        sqs_consumer=graph.sqs_consumer,
        sqs_message_handlers=graph.config.sqs_message_dispatcher.mappings,
    )
