"""
Dispatcher tests.

"""
from unittest.mock import Mock

from hamcrest import (
    assert_that,
    calling,
    equal_to,
    is_,
    raises,
)

from microcosm_pubsub.conventions import created
from microcosm_pubsub.message import SQSMessage
from microcosm_pubsub.errors import TTLExpired
from microcosm_pubsub.tests.fixtures import (
    ExampleDaemon,
    DerivedSchema,
)


MESSAGE_ID = "message-id"


def test_handle():
    """
    Test that the dispatcher handles a message and assigns context.

    """
    daemon = ExampleDaemon.create_for_testing()
    graph = daemon.graph

    content = dict(bar="baz")
    sqs_message_context = Mock(return_value=dict())
    with graph.opaque.initialize(sqs_message_context, content):
        result = graph.sqs_message_dispatcher.handle_message(
            message=SQSMessage(
                consumer=None,
                content=content,
                media_type=DerivedSchema.MEDIA_TYPE,
                message_id=MESSAGE_ID,
                receipt_handle=None,
            ),
            bound_handlers=daemon.bound_handlers,
        )

    assert_that(result, is_(equal_to(True)))
    sqs_message_context.assert_called_once_with(content)


def test_handle_with_no_context():
    """
    Test that when no context is added the dispatcher behaves sanely.

    """
    daemon = ExampleDaemon.create_for_testing()
    graph = daemon.graph

    # remove the sqs_message_context from the graph so we can test the dispatcher
    # defaulting logic
    graph._registry.entry_points.pop("sqs_message_context")

    content = dict(bar="baz")
    result = graph.sqs_message_dispatcher.handle_message(
        message=SQSMessage(
            consumer=None,
            content=content,
            media_type=DerivedSchema.MEDIA_TYPE,
            message_id=MESSAGE_ID,
            receipt_handle=None,
        ),
        bound_handlers=daemon.bound_handlers,
    )

    assert_that(result, is_(equal_to(True)))
    assert_that(graph.sqs_message_dispatcher.sqs_message_context(content), is_(equal_to({
        "X-Request-Ttl": "31",
    })))


def test_handle_with_skipping():
    """
    Test that skipping works

    """
    daemon = ExampleDaemon.create_for_testing()
    graph = daemon.graph

    content = dict(bar="baz")
    result = graph.sqs_message_dispatcher.handle_message(
        message=SQSMessage(
            consumer=None,
            content=content,
            media_type=created("bar"),
            message_id=MESSAGE_ID,
            receipt_handle=None,
        ),
        bound_handlers=daemon.bound_handlers,
    )
    assert_that(result, is_(equal_to(False)))


def test_dispatcher_sets_ttl():
    """
    Test that the dispatcher handles a message and sets the context TTL if is not sets.

    """
    daemon = ExampleDaemon.create_for_testing()
    graph = daemon.graph

    content = dict()
    sqs_message_context = Mock(return_value=dict())
    with graph.opaque.initialize(sqs_message_context, content):
        result = graph.sqs_message_dispatcher.handle_message(
            message=SQSMessage(
                consumer=None,
                content=content,
                media_type=DerivedSchema.MEDIA_TYPE,
                message_id=MESSAGE_ID,
                receipt_handle=None,
            ),
            bound_handlers=daemon.bound_handlers,
        )

    assert_that(result, is_(equal_to(True)))
    sqs_message_context.assert_called_once_with(content)
    assert_that(content, is_(equal_to({'X-Request-TTL': 31})))


def test_dispatcher_updates_ttl():
    """
    Test that the dispatcher handles a message and updates the context TTL.

    """
    daemon = ExampleDaemon.create_for_testing()
    graph = daemon.graph

    content = {'X-Request-TTL': 10}
    sqs_message_context = Mock(return_value=dict())
    with graph.opaque.initialize(sqs_message_context, content):
        result = graph.sqs_message_dispatcher.handle_message(
            message=SQSMessage(
                consumer=None,
                content=content,
                media_type=DerivedSchema.MEDIA_TYPE,
                message_id=MESSAGE_ID,
                receipt_handle=None,
            ),
            bound_handlers=daemon.bound_handlers,
        )

    assert_that(result, is_(equal_to(True)))
    sqs_message_context.assert_called_once_with(content)
    assert_that(content, is_(equal_to({'X-Request-TTL': 9})))


def test_dispatcher_fails_for_zero_ttl():
    """
    Test that the dispatcher handles a message and updates the context TTL.

    """
    daemon = ExampleDaemon.create_for_testing()
    graph = daemon.graph

    content = {'X-Request-TTL': 0}
    sqs_message_context = Mock(return_value=dict())
    with graph.opaque.initialize(sqs_message_context, content):
        assert_that(
            calling(graph.sqs_message_dispatcher.handle_message).with_args(
                message=SQSMessage(
                    consumer=None,
                    content=content,
                    media_type=DerivedSchema.MEDIA_TYPE,
                    message_id=MESSAGE_ID,
                    receipt_handle=None,
                ),
                bound_handlers=daemon.bound_handlers,
            ),
            raises(TTLExpired),
        )
