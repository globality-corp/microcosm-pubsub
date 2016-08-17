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

from microcosm_pubsub.tests.fixtures import (
    FOO_QUEUE_URL,
    FooSchema,
)


def test_handle():
    """
    Consumer delegates to SQS client.

    """
    def loader(metadata):
        return dict(
            sqs_consumer=dict(
                sqs_queue_url=FOO_QUEUE_URL,
            ),
            pubsub_message_codecs=dict(
                default=FooSchema,
            ),
        )

    graph = create_object_graph("example", testing=True, loader=loader, registry=Registry())

    @binding("sqs_message_handlers", graph._registry)
    def configure_message_handlers(graph):
        return {
            FooSchema.MEDIA_TYPE: Mock(return_value=True),
        }

    graph.use(
        'sqs_message_handlers',
    )

    message = dict(bar="baz")
    sqs_message_context = Mock(return_value=dict())
    graph.sqs_message_dispatcher.sqs_message_context = sqs_message_context

    result = graph.sqs_message_dispatcher.handle_message(FooSchema.MEDIA_TYPE, message)

    assert_that(result, is_(equal_to(True)))
    sqs_message_context.assert_called_once_with(message)


def test_handle_with_no_context():
    """
    Consumer delegates to SQS client.

    """
    def loader(metadata):
        return dict(
            sqs_consumer=dict(
                sqs_queue_url=FOO_QUEUE_URL,
            ),
            pubsub_message_codecs=dict(
                default=FooSchema,
            ),
        )
    graph = create_object_graph("example", testing=True, loader=loader, registry=Registry())

    @binding("sqs_message_handlers", graph._registry)
    def configure_message_handlers(graph):
        return {
            FooSchema.MEDIA_TYPE: Mock(return_value=True),
        }
    graph.use(
        'sqs_message_handlers',
    )

    # remove the sqs_message_context from the graph so we can test the dispatcher
    # defaulting logic
    graph._registry.entry_points.pop('sqs_message_context')

    message = dict(bar="baz")
    result = graph.sqs_message_dispatcher.handle_message(FooSchema.MEDIA_TYPE, message)

    assert_that(result, is_(equal_to(True)))
    assert_that(graph.sqs_message_dispatcher.sqs_message_context(message), is_(equal_to(dict())))
