"""
Test metrics enablement.

"""
from unittest.mock import patch

from hamcrest import assert_that, equal_to, is_
from microcosm.api import create_object_graph, load_from_dict

from microcosm_pubsub.metrics import PubSubSendMetrics


def test_configure_metrics_default_metrics_not_installed():
    """
    Disable metrics by default if metrics not installed.

    """
    with patch.object(PubSubSendMetrics, "get_metrics") as mocked:
        mocked.return_value = None

        graph = create_object_graph("example", testing=True)
        assert_that(graph.pubsub_send_metrics.enabled, is_(equal_to(False)))


def test_configure_metrics_default_metrics_installed():
    """
    Enabled metrics by default if installed.

    """
    with patch.object(PubSubSendMetrics, "get_metrics") as mocked:
        mocked.return_value = object()

        graph = create_object_graph("example", testing=True)
        assert_that(graph.pubsub_send_metrics.enabled, is_(equal_to(True)))


def test_configure_metrics_disable():
    """
    Disable metrics explicitly.

    """
    loader = load_from_dict(
        pubsub_send_metrics=dict(
            enabled=False,
        ),
    )
    graph = create_object_graph("example", testing=True, loader=loader)
    assert_that(graph.pubsub_send_metrics.enabled, is_(equal_to(False)))
