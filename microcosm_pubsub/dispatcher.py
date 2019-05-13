"""
Process batches of messages.

"""
from typing import List

from inflection import titleize
from microcosm.api import defaults, typed
from microcosm_logging.decorators import context_logger, logger
from microcosm_logging.timing import elapsed_time

from microcosm_pubsub.context import TTL_KEY
from microcosm_pubsub.errors import IgnoreMessage, SkipMessage, TTLExpired
from microcosm_pubsub.result import MessageHandlingResult, MessageHandlingResultType


@logger
@defaults(
    # Number of failed attempts after which the message stops being processed
    message_max_processing_attempts=typed(int, default_value=None)
)
class SQSMessageDispatcher:
    """
    Dispatch batches of SQSMessages to handler functions.

    """
    def __init__(self, graph):
        self.opaque = graph.opaque
        self.sqs_consumer = graph.sqs_consumer
        self.sqs_message_context = graph.sqs_message_context
        self.sqs_message_handler_registry = graph.sqs_message_handler_registry
        self.send_metrics = graph.pubsub_send_metrics
        self.send_batch_metrics = graph.pubsub_send_batch_metrics
        self.max_processing_attempts = graph.config.sqs_message_dispatcher.message_max_processing_attempts

    def handle_batch(self, bound_handlers) -> List[MessageHandlingResult]:
        """
        Send a batch of messages to a function.

        """
        with elapsed_time(self.opaque):
            instances = [
                self.handle_message(message, bound_handlers)
                for message in self.sqs_consumer.consume()
            ]

        message_count = len([
            instance for instance in instances
            if instance.result != MessageHandlingResultType.IGNORED
        ])

        self.logger.info(
            "Finished handling batch",
            extra=dict(
                message_count=message_count,
                **self.opaque.as_dict(),
            ),
        )

        self.send_batch_metrics(self.opaque["elapsed_time"], message_count)

        for instance in instances:
            self.send_metrics(instance)

        return instances

    def handle_message(self, message, bound_handlers) -> MessageHandlingResult:
        """
        Handle a message.

        """
        with self.opaque.initialize(self.sqs_message_context, message):
            handler = None
            with elapsed_time(self.opaque):
                try:
                    self.validate_message(message)
                    handler = self.find_handler(message, bound_handlers)
                    instance = MessageHandlingResult.invoke(
                        handler=self.wrap_handler(handler),
                        message=message,
                    )
                except Exception as error:
                    instance = MessageHandlingResult.from_error(
                        message=message,
                        error=error,
                    )

            instance.elapsed_time = self.opaque["elapsed_time"]
            instance.log(
                logger=self.choose_logger(handler),
                opaque=self.opaque,
            )
            instance.resolve(message)
            return instance

    def validate_message(self, message):
        self.validate_ttl()
        self.validate_processing_limit(message)
        self.validate_content(message)

    def validate_ttl(self):
        """
        Validate that the current message is not expired.

        :raises TTLExpired
        """
        if TTL_KEY not in self.opaque:
            return

        if int(self.opaque[TTL_KEY]) <= 0:
            raise TTLExpired()

    def validate_processing_limit(self, message):
        if self.max_processing_attempts and message.approximate_receive_count > self.max_processing_attempts:
            raise SkipMessage(
                "Message exceeded maximum of {max} processing attempts. Skipping",
                extra=dict(max=self.max_processing_attempts),
            )

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

    def wrap_handler(self, handler):
        """
        Wrap handler with context logger.

        Ensures that all handler logger calls have access to opaque data.

        """
        return context_logger(
            context_func=lambda *args, **kwargs: self.opaque,
            func=handler,
            parent=handler,
        )

    def choose_logger(self, handler):
        """
        Choose a logger to use for handler results.

        """
        try:
            # use the handler's logger, if possible
            return handler.logger
        except AttributeError:
            return self.logger
