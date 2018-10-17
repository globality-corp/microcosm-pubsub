from functools import partial

from microcosm.api import defaults, typed
from microcosm.config.types import boolean
from microcosm_pubsub.result import MessageHandlingResultType


def do_nothing(dummy, result):
    return


def send_metrics(metrics, result):
    if result.result == MessageHandlingResultType.IGNORED:
        return
    tags = [
        "source:micrcocosm-pubsub",
        f"result:{result.result}",
        f"media-type:{result.media_type}",
    ]
    metrics.histogram(
        "message",
        result.elapsed_time,
        tags=tags,
    )


@defaults(
    enable=typed(boolean, default_value=False)
)
def configure_pubsub_metrics(graph):
    enable_metrics = graph.config.pubsub_send_metrics.enable
    if not enable_metrics:
        return partial(do_nothing, None)
    else:
        return partial(send_metrics, graph.metrics)
