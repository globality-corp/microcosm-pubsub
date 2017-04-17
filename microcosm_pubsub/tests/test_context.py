"""
Context tests.

"""
from hamcrest import (
    assert_that,
    equal_to,
    is_,
)
from microcosm_pubsub.tests.fixtures import ExampleDaemon


MESSAGE_ID = "message_id"


def test_handle():
    daemon = ExampleDaemon.create_for_testing()
    graph = daemon.graph

    message = dict(opaque_data=dict(foo="bar"))
    with graph.opaque.initialize(graph.sqs_message_context, message, message_id=MESSAGE_ID):
        assert_that(graph.opaque.as_dict(), is_(equal_to(dict(
            foo="bar",
            message_id=MESSAGE_ID,
        ))))
