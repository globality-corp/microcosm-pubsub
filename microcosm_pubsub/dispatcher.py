"""
Process batches of messages.

"""
from collections import namedtuple

from microcosm.errors import NotBoundError
from microcosm_logging.decorators import context_logger, logger

from microcosm_pubsub.errors import Nack, SkipMessage


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

    def handle_batch(self, bound_handlers):
        """
        Send a batch of messages to a function.

        :returns: a `DispatchResult` with metrics around message consumption
        """
        message_count = error_count = ignore_count = 0
        for message in self.sqs_consumer.consume():
            message_count += 1
            try:
                with message:
                    handled = self.handle_message(
                        message_id=message.message_id,
                        media_type=message.media_type,
                        content=message.content,
                        bound_handlers=bound_handlers,
                    )
                    if not handled:
                        ignore_count += 1
            except Exception:
                error_count += 1

        return DispatchResult(message_count, error_count, ignore_count)

    def handle_message(self, message_id, media_type, content, bound_handlers):
        """
        Handle a single message.

        """
        if content is None:
            self.logger.debug("Skipping message with unparsed type: {}".format(media_type))
            return False

        with self.opaque.initialize(self.sqs_message_context, content, message_id=message_id):
            try:
                handler = self.sqs_message_handler_registry.find(media_type, bound_handlers)
            except KeyError:
                # no handlers
                self.logger.debug("Skipping message with no registered handler: {}".format(media_type))
                return False

            return self.invoke_handler(handler, media_type, content)

    def invoke_handler(self, handler, media_type, content):
        """
        Invoke handler with logging and error handling.

        """
        logger = self.choose_logger(handler)

        try:
            handler_with_context = context_logger(
                self.sqs_message_context,
                handler,
                parent=handler,
            )
            return handler_with_context(content)
        except SkipMessage as skipped:
            extra = self.sqs_message_context(content)
            extra.update(skipped.extra)
            logger.info(
                "Skipping message for reason: {}".format(str(skipped)),
                extra=extra,
            )
            return False
        except Nack:
            logger.info(
                "Nacking SQS message: {}".format(
                    media_type,
                ),
                extra=self.sqs_message_context(content)
            )
            raise
        except Exception as error:
            logger.warning(
                "Error handling SQS message: {}".format(
                    media_type,
                ),
                exc_info=True,
                extra=self.sqs_message_context(content)
            )
            raise error

    def choose_logger(self, handler):
        # NB if possible, log with the handler's logger to make it easier
        # to tell which handler failed in the logs.
        try:
            return handler.logger
        except AttributeError:
            return self.logger


def configure(graph):
    return SQSMessageDispatcher(graph)
