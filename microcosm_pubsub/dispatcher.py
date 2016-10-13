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
                if isinstance(error, Nack):
                    self.logger.info(
                        "Nacking SQS message: {}".format(
                            message.media_type,
                        ),
                        extra=self.sqs_message_context(message.content)
                    )
                else:
                    self.logger.info(
                        "Error handling SQS message: {}".format(
                            message.media_type,
                        ),
                        exc_info=True,
                        extra=self.sqs_message_context(message.content)
                    )
                error_count += 1

        return DispatchResult(message_count, error_count, ignore_count)

    def handle_message(self, media_type, message):
        """
        Handle a single message.

        """
        if message is None:
            self.logger.debug("Skipping message with unparsed type: {}".format(media_type))
            return False

        with self.opaque.initialize(self.sqs_message_context, message):
            try:
                sqs_message_handler = self.sqs_message_handler_registry[media_type]
            except KeyError:
                # no handlers
                self.logger.debug("Skipping message with no registered handler: {}".format(media_type))
                return False
            else:
                handler_with_context = context_logger(
                    self.sqs_message_context,
                    sqs_message_handler,
                    parent=sqs_message_handler,
                )
                return handler_with_context(message)


def configure(graph):
    if graph.metadata.testing:
        from mock import MagicMock
        graph.opaque = MagicMock(bind=MagicMock())

    return SQSMessageDispatcher(graph)
