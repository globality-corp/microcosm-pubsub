"""
Process batches of messages.

"""
from collections import namedtuple
from logging import getLogger

from microcosm.api import defaults
from microcosm.errors import NotBoundError
from microcosm_logging.decorators import context_logger


DispatchResult = namedtuple("DispatchResult", ["message_count", "error_count", "ignore_count"])


logger = getLogger("pubsub.sink")


class SQSMessageDispatcher(object):
    """
    Dispatch batches of SQSMessages to handler functions.

    """
    def __init__(self, sqs_consumer, sqs_message_handlers, log_with_context, message_context):
        self.sqs_consumer = sqs_consumer
        self.sqs_message_handlers = sqs_message_handlers
        self.log_with_context = log_with_context
        self.message_context = message_context

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
            except Exception:
                logger.info("Error handling SQS message.", exc_info=True)
                error_count += 1

        return DispatchResult(message_count, error_count, ignore_count)

    def handle_message(self, media_type, message):
        """
        Handle a single message.

        """
        if message is None:
            logger.debug("Skipping message with unparsed type: {}".format(media_type))
            return False
        sqs_message_handler = self.sqs_message_handlers.get(media_type)
        if sqs_message_handler is None:
            logger.debug("Skipping message with unsupported type: {}".format(media_type))
            return False

        if self.log_with_context:
            handler_with_context = context_logger(
                self.message_context,
                sqs_message_handler,
                parent=sqs_message_handler,
            )
            return handler_with_context(message)
        else:
            return sqs_message_handler(message)


@defaults(
    mappings=dict(),
    log_with_context=True,
)
def configure_sqs_message_dispatcher(graph):
    """
    Configure dispatching of SQS messages.

    Requires a dictionary mapping from the message media type to a handler function; unmapped
    messages will be ignored. This dictionary can either be defined using the `sqs_message_dispatcher.mappings`
    configuration key (which is convenient for simple handlers) or, more likely, via a separate
    `sqs_message_handlers` graph component (which is convenient of the handlers need other component collaborators).
    """
    try:
        sqs_message_handlers = graph.sqs_message_handlers
    except NotBoundError:
        sqs_message_handlers = graph.config.sqs_message_dispatcher.mappings

    return SQSMessageDispatcher(
        sqs_consumer=graph.sqs_consumer,
        sqs_message_handlers=sqs_message_handlers,
        log_with_context=graph.config.sqs_message_dispatcher.log_with_context,
        message_context=graph.message_context,
    )
