"""
Dispatcher tests.

"""
from hamcrest import (
    assert_that,
    equal_to,
    is_,
)
from microcosm.api import binding, create_object_graph
from microcosm.registry import Registry
from mock import Mock

from microcosm_pubsub.errors import SkipMessage
from microcosm_pubsub.tests.fixtures import (
    foo_handler,
    FOO_QUEUE_URL,
    FooSchema,
)


def skipping_handler(message):
    raise SkipMessage("Failed")


def test_handle():
    """
    Test that the dispatcher handles a message and assigns context.

    """
    def loader(metadata):
        return dict(
            sqs_consumer=dict(
                sqs_queue_url=FOO_QUEUE_URL,
            ),
        )

    graph = create_object_graph("example", testing=True, loader=loader, registry=Registry())

    @binding("sqs_message_handlers", graph._registry)
    def configure_message_handlers(graph):
        return {
            FooSchema.MEDIA_TYPE: foo_handler,
        }

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
    def loader(metadata):
        return dict(
            sqs_consumer=dict(
                sqs_queue_url=FOO_QUEUE_URL,
            ),
        )
    graph = create_object_graph("example", testing=True, loader=loader, registry=Registry())

    @binding("sqs_message_handlers", graph._registry)
    def configure_message_handlers(graph):
        return {
            FooSchema.MEDIA_TYPE: foo_handler,
        }

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
    def loader(metadata):
        return dict(
            sqs_consumer=dict(
                sqs_queue_url=FOO_QUEUE_URL,
            ),
        )
    graph = create_object_graph("example", testing=True, loader=loader, registry=Registry())

    @binding("sqs_message_handlers", graph._registry)
    def configure_message_handlers(graph):
        return {
            FooSchema.MEDIA_TYPE: skipping_handler,
        }

    message = dict(bar="baz")
    result = graph.sqs_message_dispatcher.handle_message(FooSchema.MEDIA_TYPE, message)
    assert_that(result, is_(equal_to(False)))
