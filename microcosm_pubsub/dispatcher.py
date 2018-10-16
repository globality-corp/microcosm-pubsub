"""
Process batches of messages.

"""
from typing import List

from inflection import titleize
from microcosm_logging.decorators import context_logger, logger
from microcosm_logging.timing import elapsed_time

from microcosm_pubsub.context import TTL_KEY
from microcosm_pubsub.errors import IgnoreMessage, Nack, SkipMessage, TTLExpired
from microcosm_pubsub.result import MessageHandlingResult


@logger
class SQSMessageDispatcher:
    """
    Dispatch batches of SQSMessages to handler functions.

    """
    def __init__(self, graph):
        self.opaque = graph.opaque
        self.sqs_consumer = graph.sqs_consumer
        self.sqs_message_context = graph.sqs_message_context
        self.sqs_message_handler_registry = graph.sqs_message_handler_registry

    def handle_batch(self, bound_handlers) -> List[MessageHandlingResult]:
        """
        Send a batch of messages to a function.

        """
        return [
            self.handle_message(message, bound_handlers)
            for message in self.sqs_consumer.consume()
        ]

    def handle_message(self, message, bound_handlers) -> MessageHandlingResult:
        """
        Handle a message.

        """
        # initialize a context for the current message
        with self.opaque.initialize(self.sqs_message_context, message):
            # save elapsed time information to the context
            with elapsed_time(self.opaque):
                message_handling_result = MessageHandlingResult.invoke(
                    func=self._handle_message,
                    message=message,
                    bound_handlers=bound_handlers,
                )
            message_handling_result.elapsed_time = self.opaque["elapsed_time"]
        return message_handling_result

    def _handle_message(self, message, bound_handlers) -> bool:
        """
        Handle a single message.

        :raises: Nack, IgnoreMessage, SkipMessage, TTLExpired

        """
        # we might just want to ignore the message
        self.validate_ttl()
        content = self.validate_content(message)
        handler = self.find_handler(message, bound_handlers)

        # we might want to handle the message
        result = self.invoke_handler(handler, message.media_type, content)

        self.logger.info(
            "Handled {handler}",
            extra=self.opaque.as_dict(),
        )
        return bool(result)

    def validate_ttl(self):
        """
        Validate that the current message is not expired.

        :raises TTLExpired
        """
        if TTL_KEY not in self.opaque:
            return

        if int(self.opaque[TTL_KEY]) <= 0:
            raise TTLExpired()

    def validate_content(self, message):
        """
        Extract message content.

        The `CodecMediaTypeAndContentParser` (used by the default `CodecSQSEnvelope`) will return
        `None` for content if it doesn't have a registered media type schema. In most cases, this
        condition will coincide with not having a registerd handler as well. Plus, we can't handle
        absent content.

        """
        if message.content is None:
            raise IgnoreMessage(f"Could not parse message for: {message.media_type}")

        return message.content

    def find_handler(self, message, bound_handlers):
        """
        Find the handler for a message.

        """
        try:
            handler = self.sqs_message_handler_registry.find(message.media_type, bound_handlers)
            self.opaque["handler"] = titleize(handler.__class__.__name__)
            return handler
        except KeyError:
            raise IgnoreMessage(f"No handler was registered for: {message.media_type}")

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
            self.opaque.update(skipped.extra)
            logger.info(
                "Skipping message for reason: {}".format(str(skipped)),
                extra=self.opaque.as_dict(),
            )
            return False
        except Nack:
            logger.info(
                "Nacking SQS message: {}".format(
                    media_type,
                ),
                extra=self.opaque.as_dict(),
            )
            raise
        except TTLExpired:
            self.logger.warning(
                "Error handling SQS message - TTL expired",
                extra=self.opaque.as_dict(),
            )
            raise
        except Exception as error:
            logger.warning(
                "Error handling SQS message: {}".format(
                    media_type,
                ),
                exc_info=True,
                extra=self.opaque.as_dict(),
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
