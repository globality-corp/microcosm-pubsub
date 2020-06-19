"""
Sentry integration tests.

"""
from hamcrest import assert_that, equal_to, is_
from microcosm.api import create_object_graph
from microcosm.decorators import binding

from microcosm_pubsub.sentry import default_before_send
from microcosm_pubsub.tests.sentry_fixture import sample_event


def test_sentry_config_bad_dsn():
    def loader(metadata):
        return dict(
            sentry_logging=dict(
                enabled=True,
                dsn="invalid schema",
            ),
            build_info_convention=dict(
                sha1="some-sha",
            ),
        )

    graph = create_object_graph("example", testing=False, loader=loader)
    assert_that(graph.sentry_logging_pubsub.enabled, is_(equal_to(False)))


def test_sentry_client_loaded_to_graph_testing():
    def loader(metadata):
        return dict(
            sentry_logging_pubsub=dict(
                dsn="topic",
                enabled=True,
            ),
        )

    graph = create_object_graph("example", testing=True, loader=loader)
    assert_that(
        graph.sentry_logging_pubsub.dsn,
        is_(equal_to("topic")),
    )


def test_sentry_client_default_to_None_no_dsn_set():
    def loader(metadata):
        return dict()

    graph = create_object_graph("example", testing=True, loader=loader)

    assert_that(
        graph.sentry_logging_pubsub.enabled,
        is_(equal_to(False)),
    )


def test_before_send_data_removal():
    filtered_event = default_before_send(sample_event, None)
    filtered_stacktrace = filtered_event["exception"]["values"][0]["stacktrace"]["frames"]

    assert_that(
        filtered_stacktrace[0]["vars"],
        is_(
            equal_to(
                {
                    "cls": "<class 'microcosm_pubsub.result.MessageHandlingResult'>",
                    "handler": "<function context_logger.<locals>.wrapped at 0x109422ef0>",
                    "message": "<microcosm_pubsub.message.SQSMessage object at 0x10952bcd0>",
                },
            ),
        ),
    )
    assert_that(
        filtered_stacktrace[1]["vars"],
        is_(
            equal_to(
                {
                    "self": "<test.daemon.test_daemon.handlers.test_handler.TestHandler object at 0x10953cbd0>",
                    "something_id": "'1f40066c-f457-41b3-aa4c-72cdac5146e4'",
                    "project_description": "<redacted>",
                    "other_id": "'70375dff-2d46-40c4-a1d1-f5d49a25698d'",
                },
            ),
        ),
    )


def test_custom_before_send():
    @binding("sentry_before_send")
    def custom_before_send_factory(graph):
        def before_send(event, hint):
            return "TESTING"
        return before_send

    def loader(metadata):
        return dict(
            sentry_logging_pubsub=dict(
                dsn="topic",
                enabled=True,
                custom_before_send_func="microcosm_pubsub.tests.test_sentry._test_custom_before_send",
            ),
        )

    graph = create_object_graph("example", testing=True, loader=loader)
    graph.use("sentry_before_send")

    client = graph.sentry_logging_pubsub.client
    assert_that(client.default_before_send(1, 1), "TESTING")
