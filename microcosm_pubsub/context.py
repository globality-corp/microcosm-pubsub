"""
Message context.

"""

from microcosm.api import defaults, typed
from microcosm.config.types import boolean
from microcosm_logging.decorators import logger

from microcosm_pubsub.errors import TTLExpired


@logger
class SQSMessageContext:
    """
    The message context controls what data you want to associate
    with your daemon handler context, e.g. some combination of keys from the message or
    additional metadata fetched from elsewhere.

    """
    def __init__(self, graph):
        self.enable_ttl = graph.config.sqs_message_context.enable_ttl
        self.initial_ttl = graph.config.sqs_message_context.initial_ttl

    def __call__(self, message_dct, **kwargs):
        context = message_dct.get("opaque_data", dict())

        self.update_context_uri(context, message_dct)
        self.update_context_ttl(context)

        context.update(**kwargs)
        return context

    def update_context_uri(self, context, message_dct):
        # If there is a uri add it to the context
        if message_dct.get("uri"):
            context["uri"] = message_dct.get("uri")

    def update_context_ttl(self, context):
        if self.enable_ttl:
            try:
                ttl = int(context["X-Request-Ttl"])
            except KeyError:
                ttl = self.initial_ttl
            if ttl == 0:
                self.logger.warning(
                    "Error handling SQS message - TTL expired",
                    extra=context,
                )
                raise TTLExpired(extra=context)
            context["X-Request-Ttl"] = str(ttl - 1)


@defaults(
    enable_ttl=typed(boolean, default_value=True),
    initial_ttl=typed(int, default_value=32),
)
def configure_sqs_message_context(graph):
    """
    Configure the message context object.

    Usage:
        graph.message_context()

    """
    return SQSMessageContext(graph)
