"""
Process batches of messages.

"""
from typing import List

from inflection import titleize
from microcosm_logging.decorators import context_logger, logger
from microcosm_logging.timing import elapsed_time

from microcosm_pubsub.context import TTL_KEY
from microcosm_pubsub.errors import IgnoreMessage, TTLExpired
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
            # save elapsed time
            message_handling_result.elapsed_time = self.opaque["elapsed_time"]
            # log handler outcome
            logger = self.choose_logger(message.handler)
            message_handling_result.log(logger, self.opaque)
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

        # handle the message
        result = handler(content)
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
            # XXX awkward
            message.handler = handler
            self.opaque["handler"] = titleize(handler.__class__.__name__)
            return context_logger(
                lambda *args, **kwargs: self.opaque,
                handler,
                parent=handler,
            )
        except KeyError:
            raise IgnoreMessage(f"No handler was registered for: {message.media_type}")

    def choose_logger(self, handler):
        """
        Choose a logger to use for handler results.

        """
        try:
            # use the handler's logger, if possible
            return handler.logger
        except AttributeError:
            return self.logger


def configure(graph):
    return SQSMessageDispatcher(graph)
