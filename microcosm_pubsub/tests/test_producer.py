"""
Producer tests.

"""
from hamcrest import (
    assert_that,
    calling,
    equal_to,
    is_,
    raises,
)
from microcosm.api import create_object_graph

from microcosm_pubsub.errors import TopicNotDefinedError
from microcosm_pubsub.tests.fixtures import (
    FOO_TOPIC,
    FOO_MEDIA_TYPE,
    FooSchema,
    MESSAGE_ID,
)


def test_produce_no_topic_arn():
    """
    Producer delegates to SNS client.

    """
    def loader(metadata):
        return dict(
            pubsub_message_codecs=dict(
                default=FooSchema,
            ),
            sns_topic_arns=dict(
            ),
        )

    graph = create_object_graph("example", testing=True, loader=loader)
    assert_that(
        calling(graph.sns_producer.produce).with_args(FOO_MEDIA_TYPE, bar="baz"),
        raises(TopicNotDefinedError),
    )


def test_produce_default_topic():
    """
    Producer delegates to SNS client.

    """
    def loader(metadata):
        return dict(
            pubsub_message_codecs=dict(
                default=FooSchema,
            ),
            sns_topic_arns=dict(
                default=FOO_TOPIC,
            )
        )

    graph = create_object_graph("example", testing=True, loader=loader)

    # set up response
    graph.sns_producer.sns_client.publish.return_value = dict(MessageId=MESSAGE_ID)

    message_id = graph.sns_producer.produce(FOO_MEDIA_TYPE, bar="baz")

    graph.sns_producer.sns_client.publish.assert_called_with(
        TopicArn='foo-topic',
        Message='{"bar": "baz", "mediaType": "application/vnd.globality.pubsub.foo"}',
    )
    assert_that(message_id, is_(equal_to(MESSAGE_ID)))


def test_produce_custom_topic():
    """
    Producer delegates to SNS client.

    """
    def loader(metadata):
        return dict(
            pubsub_message_codecs=dict(
                default=FooSchema,
            ),
            sns_topic_arns=dict(
                default=None,
                mappings={
                    FOO_MEDIA_TYPE: FOO_TOPIC,
                },
            )
        )

    graph = create_object_graph("example", testing=True, loader=loader)

    # set up response
    graph.sns_producer.sns_client.publish.return_value = dict(MessageId=MESSAGE_ID)

    message_id = graph.sns_producer.produce(FOO_MEDIA_TYPE, bar="baz")

    graph.sns_producer.sns_client.publish.assert_called_with(
        TopicArn='foo-topic',
        Message='{"bar": "baz", "mediaType": "application/vnd.globality.pubsub.foo"}',
    )
    assert_that(message_id, is_(equal_to(MESSAGE_ID)))
