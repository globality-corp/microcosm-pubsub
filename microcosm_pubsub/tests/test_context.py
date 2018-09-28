"""
Context tests.

"""
from hamcrest import (
    assert_that,
    calling,
    has_entries,
    raises,
)
from microcosm_pubsub.errors import TTLExpired
from microcosm_pubsub.tests.fixtures import ExampleDaemon


MESSAGE_ID = "message_id"
MESSAGE_URI = "message_uri"


def test_handle():
    daemon = ExampleDaemon.create_for_testing()
    graph = daemon.graph

    message = dict(
        opaque_data=dict(foo="bar"),
        uri=MESSAGE_URI,
    )
    with graph.opaque.initialize(graph.sqs_message_context, message, message_id=MESSAGE_ID):
        assert_that(graph.opaque.as_dict(), has_entries(dict(
            foo="bar",
            message_id=MESSAGE_ID,
            uri=MESSAGE_URI,
        )))


def test_sets_ttl():
    daemon = ExampleDaemon.create_for_testing()
    graph = daemon.graph
    message = {}

    with graph.opaque.initialize(graph.sqs_message_context, message, message_id=MESSAGE_ID):
        assert_that(graph.opaque.as_dict(), has_entries({
            "X-Request-Ttl": "31",
        }))


def test_updates_ttl():
    daemon = ExampleDaemon.create_for_testing()
    graph = daemon.graph
    message = {"opaque_data": {"X-Request-Ttl": "10"}}

    with graph.opaque.initialize(graph.sqs_message_context, message, message_id=MESSAGE_ID):
        assert_that(graph.opaque.as_dict(), has_entries({
            "X-Request-Ttl": "9",
        }))


def test_zero_ttl():
    daemon = ExampleDaemon.create_for_testing()
    graph = daemon.graph
    message = {"opaque_data": {"X-Request-Ttl": "0"}}

    assert_that(calling(graph.sqs_message_context).with_args(message), raises(TTLExpired))
