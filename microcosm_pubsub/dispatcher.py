"""
Process batches of messages.

Flow diagram:

+-----------------------+                        +-------------+                                            +----------+
|                       |                        |             |                                            |          |
|                       |     1. Get messages    |             |                                            |          |
| SQSMessageDispatcher  |<-----------------------+             +<-------------------------------------------+          |
|                       |                        |             |                                            |          |
|                       |                        |             |                                            |          |
+-------+-+-------------+                        |             |                                            |          |
           | ˆ                                   |             |                                            |          |
           | | 2. Get handling result            |             |                                            |          |
           | |        +-------------------+      |             |                                            |          |
           | |        |                   |      | SQSConsumer |                                            |   SQS    |
           | |        |                   |      |             |                                            |          |
           | +------->+     Handlers      |      |             |                                            |          |
           |          |                   |      |             |                                            |          |
           |          |                   |      |             |                                            |          |
           |          +-------------------+      |             |                                            |          |
           |                                     |             |                                            |          |
           |  3. Propagate result to SQS         |             |  Action taken depends on handler result:   |          |
           +------------------------------------>+             +------------------------------------------->+          |
                                                 |             |  - SUCCEED/SKIP: delete message from queue |          |
                                                 +-------------+  - FAIL/RETRY: change message visibility   +----------+
                                                                    for future reprocessing.

"""
from logging import Logger
from time import time
from typing import List

from inflection import titleize
from microcosm.api import defaults, typed
from microcosm_logging.decorators import context_logger, logger
from microcosm_logging.timing import elapsed_time

from microcosm_pubsub.constants import PUBLISHED_KEY, TTL_KEY
from microcosm_pubsub.errors import IgnoreMessage, SkipMessage, TTLExpired
from microcosm_pubsub.result import MessageHandlingResult, MessageHandlingResultType


@logger
@defaults(
    # Number of failed attempts after which the message stops being processed
    message_max_processing_attempts=typed(int, default_value=None),
)
class SQSMessageDispatcher:
    """
    Dispatch batches of SQSMessages to handler functions.

    """
    logger: Logger

    def __init__(self, graph):
        self.opaque = graph.opaque
        self.sqs_consumer = graph.sqs_consumer
        self.sqs_message_context = graph.sqs_message_context
        self.sqs_message_handler_registry = graph.sqs_message_handler_registry
        self.send_metrics = graph.pubsub_send_metrics
        self.send_batch_metrics = graph.pubsub_send_batch_metrics
        self.max_processing_attempts = graph.config.sqs_message_dispatcher.message_max_processing_attempts
        self.sentry_config = graph.sentry_logging_pubsub

    def handle_batch(self, bound_handlers) -> List[MessageHandlingResult]:
        """
        Send a batch of messages to a function.

        """
        start_time = time()

        instances = [
            self.handle_message(message, bound_handlers)
            for message in self.sqs_consumer.consume()
        ]

        batch_elapsed_time = (time() - start_time) * 1000

        message_batch_size = len([
            instance for instance in instances
            if instance.result != MessageHandlingResultType.IGNORED
        ])

        if message_batch_size > 0:
            # NB: Expose formatted message
            message = "Completed batch: Message count: {message_batch_size}, elapsed_time: {batch_elapsed_time}".format(
                message_batch_size=message_batch_size,
                batch_elapsed_time=batch_elapsed_time,
            )
            self.logger.debug(message)

        self.send_batch_metrics(batch_elapsed_time, message_batch_size)

        for instance in instances:
            self.send_metrics(instance)

        return instances

    def handle_message(self, message, bound_handlers) -> MessageHandlingResult:
        """
        Handle a message.

        """
        with self.opaque.initialize(self.sqs_message_context, message):
            handler = None

            start_handle_time = time()

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
            published_time = self.opaque.get(PUBLISHED_KEY)
            if published_time:
                instance.handle_start_time = start_handle_time - float(published_time)
            instance.log(
                logger=self.choose_logger(handler),
                opaque=self.opaque,
            )
            instance.error_reporting(
                sentry_config=self.sentry_config,
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
