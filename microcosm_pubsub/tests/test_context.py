"""
Context tests.

"""
from hamcrest import (
    assert_that,
    has_entries,
)
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
