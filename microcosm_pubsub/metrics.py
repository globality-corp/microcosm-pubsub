from distutils.util import strtobool

from microcosm.api import defaults
from microcosm_pubsub.result import MessageHandlingResultType


def do_nothing(results):
    return


@defaults(
    enable_metrics="false",
)
def configure_pubsub_metrics(graph):
    enable_metrics = strtobool(graph.config.pubsub_send_metrics.enable_metrics)
    if not enable_metrics:
        return do_nothing
    graph.use("metrics")
    tags = ["source:micrcocosm-pubsub"]

    def send_metrics(results):
        if results.result == MessageHandlingResultType.IGNORED:
            return
        extra_tags = [
            f"result:{results.result}",
            f"media-type:{results.media_type}",
        ]
        graph.metrics.increment(
            "message_count",
            tags=tags + extra_tags,
        )
        graph.metrics.histogram(
            "message",
            results.elapsed_time,
            tags=tags + extra_tags,
        )

    return send_metrics
