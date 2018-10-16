"""
Message context.

"""

from microcosm.api import defaults, typed
from microcosm.config.types import boolean
from microcosm_logging.decorators import logger

from microcosm_pubsub.errors import TTLExpired


TTL_KEY = "X-Request-Ttl"


@defaults(
    enable_ttl=typed(boolean, default_value=True),
    initial_ttl=typed(int, default_value=32),
)
@logger
class SQSMessageContext:
    """
    Factory for per-message contexts.

    """
    def __init__(self, graph):
        self.enable_ttl = graph.config.sqs_message_context.enable_ttl
        self.initial_ttl = graph.config.sqs_message_context.initial_ttl

    def __call__(self, content, **kwargs):
        """
        Create a new context from message content.

        """
        # start with opaque data passed in message
        if "opaque_data" in content:
            context = content["opaque_data"].copy()
        else:
            context = dict()

        # merge in explicit and derived arguments
        context.update(
            **self.uri_for(context, content),
            **self.ttl_for(context, content),
            **kwargs,
        )

        return context

    def uri_for(self, context, content):
        uri = content.get("uri")
        return dict(uri=uri) if uri else dict()

    def ttl_for(self, context, content):
        try:
            ttl = int(context[TTL_KEY])
        except KeyError:
            ttl = self.initial_ttl

        if ttl == 0:
            self.logger.warning(
                "Error handling SQS message - TTL expired",
                extra=context,
            )
            raise TTLExpired(extra=context)

        return {
            TTL_KEY: str(ttl - 1),
        }
