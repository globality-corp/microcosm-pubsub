"""
Process batches of messages.

"""
from contextlib import contextmanager
from typing import Dict, List, Tuple

from inflection import titleize
from microcosm_logging.decorators import context_logger, logger
from microcosm_logging.timing import elapsed_time

from microcosm_pubsub.errors import IgnoreMessage, Nack, SkipMessage
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
            self.handle_or_ignore_message(message, bound_handlers)
            for message in self.sqs_consumer.consume()
        ]

    def handle_or_ignore_message(self, message, bound_handlers) -> MessageHandlingResult:
        """
        Handle or ignore single message.

        :raises: IgnoreMessage

        """
        content = self.validate_content(message)
        handler = self.find_handler(message, bound_handlers)
        return MessageHandlingResult.invoke(
            func=self.handle_message,
            message=message,
            content=content,
            handler=handler,
            opaque=self.opaque,
        )

    def handle_message(self, message, content, handler) -> Tuple[bool, float]:
        """
        Handle a single message.

        :raises: Nack, SkipMessage, TTLExpired

        """
        with self.message_context(message, content, handler) as extra:
            with elapsed_time(extra):
                result = self.invoke_handler(handler, message.media_type, content, extra)

            self.logger.info(
                "Handled {handler}",
                extra=self.opaque.as_dict(),
            )
            return bool(result), extra["elapsed_time"]

    @contextmanager
    def message_context(self, message, content, handler) -> Dict[str, str]:
        """
        Set the message context.

        """
        with self.opaque.initialize(
            self.sqs_message_context,
            content,
            handler=titleize(handler.__class__.__name__),
            message_id=message.message_id,
        ):
            yield self.opaque.as_dict()

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
            return self.sqs_message_handler_registry.find(message.media_type, bound_handlers)
        except KeyError:
            raise IgnoreMessage(f"No handler was registered for: {message.media_type}")

    def invoke_handler(self, handler, media_type, content, extra):
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
                extra=extra,
            )
            raise
        except Exception as error:
            logger.warning(
                "Error handling SQS message: {}".format(
                    media_type,
                ),
                exc_info=True,
                extra=extra,
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
