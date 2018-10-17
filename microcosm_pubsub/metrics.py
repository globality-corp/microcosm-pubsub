from functools import partial

from microcosm.api import defaults, typed
from microcosm.config.types import boolean
from microcosm_pubsub.result import MessageHandlingResultType


def do_nothing(results):
    return


def send_metrics(metrics, results):
    if results.result == MessageHandlingResultType.IGNORED:
        return
    tags = [
        "source:micrcocosm-pubsub",
        f"result:{results.result}",
        f"media-type:{results.media_type}",
    ]
    metrics.histogram(
        "message",
        results.elapsed_time,
        tags=tags,
    )


@defaults(
    enable=typed(boolean, default_value=False)
)
def configure_pubsub_metrics(graph):
    enable_metrics = graph.config.pubsub_send_metrics.enable
    if not enable_metrics:
        return do_nothing
    else:
        return partial(send_metrics, graph.metrics)
