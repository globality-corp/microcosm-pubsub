"""
Dispatcher tests.

"""
from hamcrest import (
    assert_that,
    equal_to,
    is_,
)
from mock import Mock

from microcosm_pubsub.conventions import created
from microcosm_pubsub.tests.fixtures import (
    ExampleDaemon,
    FooSchema,
)


def test_handle():
    """
    Test that the dispatcher handles a message and assigns context.

    """
    graph = ExampleDaemon.create_for_testing().graph
    message = dict(bar="baz")
    sqs_message_context = Mock(return_value=dict())
    with graph.opaque.initialize(sqs_message_context, message):
        result = graph.sqs_message_dispatcher.handle_message(FooSchema.MEDIA_TYPE, message)

    assert_that(result, is_(equal_to(True)))
    sqs_message_context.assert_called_once_with(message)


def test_handle_with_no_context():
    """
    Test that when no context is added the dispatcher behaves sanely.

    """
    graph = ExampleDaemon.create_for_testing().graph

    # remove the sqs_message_context from the graph so we can test the dispatcher
    # defaulting logic
    graph._registry.entry_points.pop('sqs_message_context')

    message = dict(bar="baz")
    result = graph.sqs_message_dispatcher.handle_message(FooSchema.MEDIA_TYPE, message)

    assert_that(result, is_(equal_to(True)))
    assert_that(graph.sqs_message_dispatcher.sqs_message_context(message), is_(equal_to(dict())))


def test_handle_with_skipping():
    """
    Test that skipping works

    """
    graph = ExampleDaemon.create_for_testing().graph
    message = dict(bar="baz")
    result = graph.sqs_message_dispatcher.handle_message(created("bar"), message)
    assert_that(result, is_(equal_to(False)))
