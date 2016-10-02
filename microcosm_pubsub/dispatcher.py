"""
Process batches of messages.

"""
from collections import namedtuple

from microcosm.errors import NotBoundError
from microcosm_logging.decorators import context_logger, logger
from microcosm_pubsub.errors import Nack


DispatchResult = namedtuple("DispatchResult", ["message_count", "error_count", "ignore_count"])


@logger
class SQSMessageDispatcher(object):
    """
    Dispatch batches of SQSMessages to handler functions.

    """
    def __init__(self, graph):
        self.opaque = graph.opaque
        self.sqs_consumer = graph.sqs_consumer
        self.sqs_message_context = self._find_sqs_message_context(graph)
        self.sqs_message_handler_registry = graph.sqs_message_handler_registry

    def _find_sqs_message_context(self, graph):
        try:
            return graph.sqs_message_context
        except NotBoundError:
            def context(message):
                return dict()
            return context

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
                    if not self.handle_message(message.media_type, message.content):
                        ignore_count += 1
            except Exception as error:
                self.logger.info(
                    "Error handling SQS message.",
                    exc_info=not isinstance(error, Nack),
                    extra=self.sqs_message_context(message.content)
                )
                error_count += 1

        return DispatchResult(message_count, error_count, ignore_count)

    def handle_message(self, media_type, message):
        """
        Handle a single message.

        """
        if message is None:
            logger.debug("Skipping message with unparsed type: {}".format(media_type))
            return False

        with self.opaque.initialize(self.sqs_message_context, message):
            for sqs_message_handler in self.sqs_message_handler_registry.iterate(media_type):
                handler_with_context = context_logger(
                    self.sqs_message_context,
                    sqs_message_handler,
                    parent=sqs_message_handler,
                )
                return handler_with_context(message)
            else:
                # no handlers
                return False


def configure(graph):
    if graph.metadata.testing:
        from mock import MagicMock
        graph.opaque = MagicMock(bind=MagicMock())

    return SQSMessageDispatcher(graph)
