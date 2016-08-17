"""
Consumer tests.

"""
from hamcrest import (
    assert_that,
    equal_to,
    is_,
)
from microcosm.api import binding, create_object_graph
from mock import Mock

from microcosm_pubsub.tests.fixtures import (
    FOO_QUEUE_URL,
    FooSchema,
)


def test_handle_batch():
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

    @binding("sqs_message_handlers")
    def configure_message_handlers(graph):
        return {
            FooSchema.MEDIA_TYPE: Mock(return_value=True),
        }

    graph = create_object_graph("example", testing=True, loader=loader)
    graph.use(
        'sqs_message_handlers',
    )

    message = dict(bar="baz")
    message_context = Mock(return_value=dict())
    graph.sqs_message_dispatcher.message_context = message_context

    result = graph.sqs_message_dispatcher.handle_message(FooSchema.MEDIA_TYPE, message)

    assert_that(result, is_(equal_to(True)))
    message_context.assert_called_once_with(message)
