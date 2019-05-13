"""
Producer tests.

"""
from json import loads
from os import environ

import microcosm.opaque  # noqa
from hamcrest import (
    assert_that,
    calling,
    equal_to,
    is_,
    none,
    raises,
)
from microcosm.api import create_object_graph
from microcosm.loaders import load_from_environ

from microcosm_pubsub.batch import MessageBatchSchema
from microcosm_pubsub.conventions import created
from microcosm_pubsub.errors import TopicNotDefinedError
from microcosm_pubsub.producer import (
    DeferredBatchProducer,
    DeferredProducer,
    deferred,
    deferred_batch,
    iter_topic_mappings,
)
from microcosm_pubsub.tests.fixtures import DerivedSchema


MESSAGE_ID = "message-id"


def test_produce_no_topic_arn():
    """
    Producer delegates to SNS client.

    """
    def loader(metadata):
        return dict(
            sns_topic_arns=dict(
            ),
        )

    graph = create_object_graph("example", testing=True, loader=loader)
    assert_that(
        calling(graph.sns_producer.produce).with_args(DerivedSchema.MEDIA_TYPE, data="data"),
        raises(TopicNotDefinedError),
    )


def test_produce_default_topic():
    """
    Producer delegates to SNS client.

    """
    def loader(metadata):
        return dict(
            sns_topic_arns=dict(
                default="topic",
            )
        )

    graph = create_object_graph("example", testing=True, loader=loader)

    # set up response
    graph.sns_producer.sns_client.publish.return_value = dict(MessageId=MESSAGE_ID)

    message_id = graph.sns_producer.produce(DerivedSchema.MEDIA_TYPE, data="data")

    assert_that(graph.sns_producer.sns_client.publish.call_count, is_(equal_to(1)))
    assert_that(graph.sns_producer.sns_client.publish.call_args[1]["TopicArn"], is_(equal_to("topic")))
    assert_that(loads(graph.sns_producer.sns_client.publish.call_args[1]["Message"]), is_(equal_to({
        "data": "data",
        "mediaType": DerivedSchema.MEDIA_TYPE,
        "opaqueData": {},
    })))
    assert_that(message_id, is_(equal_to(MESSAGE_ID)))


def test_produce_custom_topic():
    """
    Producer delegates to SNS client.

    """
    def loader(metadata):
        return dict(
            sns_topic_arns=dict(
                default=None,
                mappings={
                    DerivedSchema.MEDIA_TYPE: "special-topic",
                },
            )
        )

    graph = create_object_graph("example", testing=True, loader=loader)
    graph.use("opaque")

    # set up response
    graph.sns_producer.sns_client.publish.return_value = dict(MessageId=MESSAGE_ID)

    message_id = graph.sns_producer.produce(DerivedSchema.MEDIA_TYPE, data="data")

    assert_that(graph.sns_producer.sns_client.publish.call_count, is_(equal_to(1)))
    assert_that(graph.sns_producer.sns_client.publish.call_args[1]["TopicArn"], is_(equal_to("special-topic")))
    assert_that(loads(graph.sns_producer.sns_client.publish.call_args[1]["Message"]), is_(equal_to({
        "data": "data",
        "mediaType": DerivedSchema.MEDIA_TYPE,
        "opaqueData": {},
    })))
    assert_that(message_id, is_(equal_to(MESSAGE_ID)))


def test_iter_topic_mappings():
    result = dict(
        iter_topic_mappings(
            dict(
                foo="bar",
                bar=dict(
                    foo="baz",
                ),
                baz=dict(
                    foo=dict(
                        bar="foo",
                    ),
                ),
            )
        )
    )
    assert_that(result, is_(equal_to({
        "foo": "bar",
        "bar.foo": "baz",
        "baz.foo.bar": "foo",
    })))


def test_produce_custom_topic_environ():
    """
    Can set a custom topic via environment

    """
    key = "EXAMPLE__SNS_TOPIC_ARNS__CREATED__FOO__BAR_BAZ"
    environ[key] = "topic"
    graph = create_object_graph("example", testing=True, loader=load_from_environ)
    graph.sns_producer.produce(created("foo.bar_baz"), bar="baz")
    assert_that(graph.sns_producer.sns_client.publish.call_count, is_(equal_to(1)))
    assert_that(graph.sns_producer.sns_client.publish.call_args[1]["TopicArn"], is_(equal_to("topic")))


def test_deferred_production():
    """
    Deferred production waits until the end of a block.

    """
    def loader(metadata):
        return dict(
            sns_topic_arns=dict(
                default="topic",
            )
        )

    graph = create_object_graph("example", testing=True, loader=loader)
    graph.use("opaque")

    # set up response
    graph.sns_producer.sns_client.publish.return_value = dict(MessageId=MESSAGE_ID)

    with DeferredProducer(graph.sns_producer) as producer:
        assert_that(producer.produce(DerivedSchema.MEDIA_TYPE, data="data"), is_(none()))
        assert_that(graph.sns_producer.sns_client.publish.call_count, is_(equal_to(0)))

    assert_that(graph.sns_producer.sns_client.publish.call_count, is_(equal_to(1)))


def test_deferred_production_decorator():
    """
    Deferred production can be used to decorate a function

    """
    def loader(metadata):
        return dict(
            sns_topic_arns=dict(
                default="topic",
            )
        )

    class Foo:
        def __init__(self, graph):
            self.graph = graph
            self.sns_producer = graph.sns_producer

        def bar(self):
            assert isinstance(self.sns_producer, DeferredProducer)
            self.sns_producer.produce(DerivedSchema.MEDIA_TYPE, data="data")
            assert_that(graph.sns_producer.sns_client.publish.call_count, is_(equal_to(0)))

    graph = create_object_graph("example", testing=True, loader=loader)
    foo = Foo(graph)

    func = deferred(foo)(foo.bar)
    func()

    assert_that(graph.sns_producer.sns_client.publish.call_count, is_(equal_to(1)))


def test_deferred_batch_production():
    """
    Deferred production waits until the end of a block and publishes all
    messages in one MessageBatchSchema

    """
    graph = create_object_graph("example", testing=True, loader=batch_loader)
    graph.use("opaque")

    # set up response
    graph.sns_producer.sns_client.publish.return_value = dict(MessageId=MESSAGE_ID)

    with DeferredBatchProducer(graph.sns_producer) as producer:
        assert_that(producer.produce(DerivedSchema.MEDIA_TYPE, data="data"), is_(none()))
        assert_that(producer.produce(DerivedSchema.MEDIA_TYPE, data="data2"), is_(none()))
        assert_that(graph.sns_producer.sns_client.publish.call_count, is_(equal_to(0)))

    assert_that(graph.sns_producer.sns_client.publish.call_count, is_(equal_to(1)))
    assert_that(
        loads(graph.sns_producer.sns_client.publish.call_args[1]["Message"])["mediaType"],
        is_(equal_to("application/vnd.globality.pubsub._.created.batch_message")),
    )


def test_increased_deferred_batch_production():
    """
    Deferred production caps the size of each batch of messages during the publish call

    """
    graph = create_object_graph("example", testing=True, loader=batch_loader)
    graph.use("opaque")

    # set up response
    graph.sns_producer.sns_client.publish.return_value = dict(MessageId=MESSAGE_ID)

    with DeferredBatchProducer(graph.sns_producer) as producer:
        for i in range(202):
            assert_that(producer.produce(DerivedSchema.MEDIA_TYPE, data=f"{i}"), is_(none()))
        assert_that(graph.sns_producer.sns_client.publish.call_count, is_(equal_to(0)))

    assert_that(graph.sns_producer.sns_client.publish.call_count, is_(equal_to(3)))
    assert_that(
        loads(graph.sns_producer.sns_client.publish.call_args[1]["Message"])["mediaType"],
        is_(equal_to("application/vnd.globality.pubsub._.created.batch_message")),
    )


def test_deferred_batch_with_single_message():
    graph = create_object_graph("example", testing=True, loader=batch_loader)
    graph.use("opaque")

    with DeferredBatchProducer(graph.sns_producer) as producer:
        assert_that(producer.produce(DerivedSchema.MEDIA_TYPE, data="data"), is_(none()))
        assert_that(graph.sns_producer.sns_client.publish.call_count, is_(equal_to(0)))

    assert_that(graph.sns_producer.sns_client.publish.call_count, is_(equal_to(1)))
    assert_that(
        loads(graph.sns_producer.sns_client.publish.call_args[1]["Message"])["mediaType"],
        is_(equal_to("application/vnd.microcosm.derived")),
    )


def test_deferred_batch_with_no_message():
    graph = create_object_graph("example", testing=True, loader=batch_loader)
    graph.use("opaque")

    with DeferredBatchProducer(graph.sns_producer):
        pass

    assert_that(graph.sns_producer.sns_client.publish.call_count, is_(equal_to(0)))


def test_publish_batch_with_no_topic_fails():
    """
    Require explicit configuration of a topic for batch messages.

    """
    def loader(metadata):
        return dict(
            sns_topic_arns=dict(
                default="topic",
            )
        )

    graph = create_object_graph("example", testing=True, loader=loader)
    graph.use("opaque")

    # set up response
    graph.sns_producer.sns_client.publish.return_value = dict(MessageId=MESSAGE_ID)

    assert_that(
        calling(graph.sns_producer.produce).with_args(
            MessageBatchSchema.MEDIA_TYPE,
            messages=[]
        ),
        raises(TopicNotDefinedError)
    )


def test_batch_deferred_production_decorator():
    """
    Deferred production can be used to decorate a function

    """
    class Foo:
        def __init__(self, graph):
            self.graph = graph
            self.sns_producer = graph.sns_producer

        def bar(self):
            assert isinstance(self.sns_producer, DeferredBatchProducer)
            self.sns_producer.produce(DerivedSchema.MEDIA_TYPE, data="data")
            self.sns_producer.produce(DerivedSchema.MEDIA_TYPE, data="data2")
            assert_that(graph.sns_producer.sns_client.publish.call_count, is_(equal_to(0)))

    graph = create_object_graph("example", testing=True, loader=batch_loader)
    foo = Foo(graph)

    func = deferred_batch(foo)(foo.bar)
    func()

    assert_that(graph.sns_producer.sns_client.publish.call_count, is_(equal_to(1)))


def batch_loader(metadata):
    return dict(
        sns_topic_arns=dict(
            default="topic",
            mappings={
                MessageBatchSchema.MEDIA_TYPE: "batch-topic",
            },
        )
    )
