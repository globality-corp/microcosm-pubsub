from microcosm.api import defaults, typed
from microcosm.config.types import boolean
from microcosm.errors import NotBoundError

from microcosm_pubsub.result import MessageHandlingResult, MessageHandlingResultType


@defaults(
    enabled=typed(boolean, default_value=True)
)
class PubSubSendMetrics:
    """
    Send metrics relating to a single MessageHandlingResult

    """

    def __init__(self, graph):
        self.metrics = self.get_metrics(graph)
        self.enabled = bool(
            self.metrics
            and self.metrics.host != "localhost"
            and graph.config.pubsub_send_metrics.enabled
        )

    def get_metrics(self, graph):
        """
        Fetch the metrics client from the graph.

        Metrics will be disabled if the not configured.

        """
        try:
            return graph.metrics
        except NotBoundError:
            return None

    def __call__(self, result: MessageHandlingResult):
        """
        Send metrics if enabled.

        """
        if not self.enabled:
            return

        if result.result == MessageHandlingResultType.IGNORED:
            return

        tags = [
            "source:microcosm-pubsub",
            f"result:{result.result}",
            f"media-type:{result.media_type}",
        ]
        self.metrics.histogram(
            "message",
            result.elapsed_time,
            tags=tags,
        )

        if result.handle_start_time:
            self.metrics.histogram(
                "message_handle_start",
                result.handle_start_time,
                tags=tags,
            )


@defaults(
    enabled=typed(boolean, default_value=True)
)
class PubSubSendBatchMetrics:
    """
    Send metrics relating to a batch of handled messages

    """

    def __init__(self, graph):
        self.metrics = self.get_metrics(graph)
        self.enabled = bool(
            self.metrics
            and self.metrics.host != "localhost"
            and graph.config.pubsub_send_metrics.enabled
        )

    def get_metrics(self, graph):
        """
        Fetch the metrics client from the graph.

        Metrics will be disabled if the not configured.

        """
        try:
            return graph.metrics
        except NotBoundError:
            return None

    def __call__(self, elapsed_time: float, message_batch_size: int):
        """
        Send metrics if enabled, and if the batch processed contains at least one
        non-ignored message

        """
        if not self.enabled:
            return

        if not message_batch_size > 0:
            return

        tags = [
            "source:microcosm-pubsub",
        ]

        self.metrics.histogram(
            "message_batch",
            elapsed_time,
            tags=tags,
        )

        self.metrics.histogram(
            "message_batch_size",
            message_batch_size,
            tags=tags,
        )


@defaults(
    enabled=typed(boolean, default_value=True)
)
class PubSubProducerMetrics:
    """
    Send metrics regarding the producer

    """

    def __init__(self, graph):
        self.metrics = self.get_metrics(graph)
        self.enabled = bool(
            self.metrics
            and self.metrics.host != "localhost"
            and graph.config.pubsub_send_metrics.enabled
        )

    def get_metrics(self, graph):
        """
        Fetch the metrics client from the graph.

        Metrics will be disabled if the not configured.

        """
        try:
            return graph.metrics
        except NotBoundError:
            return None

    def __call__(self, elapsed_time: float, publish_result: bool, **kwargs):
        """
        Send metrics for how long it takes to produce a message

        """
        if not self.enabled:
            return

        tags = [
            "source:microcosm-pubsub",
            f"publish_result:{publish_result}",
            f"media_type:{kwargs['media_type']}",
        ]

        self.metrics.histogram(
            "sns_producer",
            elapsed_time,
            tags=tags,
        )
