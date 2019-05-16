"""
URI and Identity message tests.

"""
from json import dumps, loads
from time import time
from unittest.mock import patch

from hamcrest import (
    assert_that,
    equal_to,
    instance_of,
    is_,
)
from microcosm.api import create_object_graph

from microcosm_pubsub.codecs import PubSubMessageCodec
from microcosm_pubsub.conventions import (
    IdentityMessageSchema,
    LifecycleChange,
    URIMessageSchema,
    created,
    deleted,
    make_media_type,
)
from microcosm_pubsub.tests.fixtures import ExampleDaemon, noop_handler


class Foo:
    pass


def test_encode_uri_message_schema():
    """
    Message encoding should include the standard fields.

    """
    schema = URIMessageSchema(make_media_type("Foo", lifecycle_change=LifecycleChange.Deleted))
    codec = PubSubMessageCodec(schema)
    assert_that(
        loads(codec.encode(
            opaque_data=dict(foo="bar"),
            uri="http://example.com",
        )),
        is_(equal_to({
            "mediaType": "application/vnd.globality.pubsub._.deleted.foo",
            "opaqueData": {
                "foo": "bar",
            },
            "uri": "http://example.com",
        })),
    )


def test_encode_identity_message_schema():
    """
    Message encoding should include the standard fields.

    """
    schema = IdentityMessageSchema(make_media_type("Foo", lifecycle_change=LifecycleChange.Deleted))
    codec = PubSubMessageCodec(schema)
    assert_that(
        loads(codec.encode(
            opaque_data=dict(foo="bar"),
            id="1",
        )),
        is_(equal_to({
            "mediaType": "application/vnd.globality.pubsub._.deleted.foo",
            "opaqueData": {
                "foo": "bar",
            },
            "id": "1",
        })),
    )


def test_decode_uri_message_schema():
    """
    Message decoding should process standard fields.

    """
    schema = URIMessageSchema(make_media_type("Foo"))
    codec = PubSubMessageCodec(schema)
    message = dumps({
        "mediaType": "application/vnd.globality.pubsub.foo",
        "opaqueData": {
            "foo": "bar",
        },
        "uri": "http://example.com",
    })
    assert_that(codec.decode(message), is_(equal_to({
        "media_type": "application/vnd.globality.pubsub.foo",
        "opaque_data": dict(foo="bar"),
        "uri": "http://example.com",
    })))


def test_decode_identity_message_schema():
    """
    Message decoding should process standard fields.

    """
    schema = IdentityMessageSchema(make_media_type("Foo"))
    codec = PubSubMessageCodec(schema)
    message = dumps({
        "mediaType": "application/vnd.globality.pubsub.foo",
        "opaqueData": {
            "foo": "bar",
        },
        "id": "1",
    })
    assert_that(codec.decode(message), is_(equal_to({
        "media_type": "application/vnd.globality.pubsub.foo",
        "opaque_data": dict(foo="bar"),
        "id": "1",
    })))


def test_publish_by_uri_convention():
    """
    Message publishing can use this convention.

    """
    def loader(metadata):
        return dict(
            sns_topic_arns=dict(
                default="default",
            )
        )

    graph = create_object_graph("example", testing=True, loader=loader)

    published_time = time()
    with patch("microcosm_pubsub.producer.time") as mocked_time:
        mocked_time.return_value = published_time
        graph.sns_producer.produce(created("foo"), uri="http://example.com", opaque_data=dict())

    assert_that(graph.sns_producer.sns_client.publish.call_count, is_(equal_to(1)))
    assert_that(graph.sns_producer.sns_client.publish.call_args[1]["TopicArn"], is_(equal_to("default")))
    assert_that(loads(graph.sns_producer.sns_client.publish.call_args[1]["Message"]), is_(equal_to({
        "mediaType": "application/vnd.globality.pubsub._.created.foo",
        "uri": "http://example.com",
        "opaqueData": {
            "X-Request-Published": published_time,
        },
    })))


def test_publish_by_identity_convention():
    """
    Message publishing can use this convention.

    """
    def loader(metadata):
        return dict(
            sns_topic_arns=dict(
                default="default",
            )
        )

    graph = create_object_graph("example", testing=True, loader=loader)

    published_time = time()
    with patch("microcosm_pubsub.producer.time") as mocked_time:
        mocked_time.return_value = published_time
        graph.sns_producer.produce(deleted("foo"), id="1", opaque_data=dict())

    assert_that(graph.sns_producer.sns_client.publish.call_count, is_(equal_to(1)))
    assert_that(graph.sns_producer.sns_client.publish.call_args[1]["TopicArn"], is_(equal_to("default")))
    assert_that(loads(graph.sns_producer.sns_client.publish.call_args[1]["Message"]), is_(equal_to({
        "mediaType": "application/vnd.globality.pubsub._.deleted.foo",
        "id": "1",
        "opaqueData": {
            "X-Request-Published": published_time,
        },
    })))


def test_dispatch_by_uri_convention():
    """
    Message dispatch can use this convention.

    """
    daemon = ExampleDaemon.create_for_testing()
    graph = daemon.graph

    media_type = created(Foo)

    assert_that(
        graph.pubsub_message_schema_registry.find(media_type).schema,
        is_(instance_of(URIMessageSchema)),
    )

    assert_that(
        graph.sqs_message_handler_registry.find(media_type, daemon.bound_handlers),
        is_(equal_to(noop_handler)),
    )


def test_dispatch_by_identity_convention():
    """
    Message dispatch can use this convention.

    """
    daemon = ExampleDaemon.create_for_testing()
    graph = daemon.graph

    media_type = deleted(Foo)

    assert_that(
        graph.pubsub_message_schema_registry.find(media_type).schema,
        is_(instance_of(IdentityMessageSchema)),
    )

    assert_that(
        graph.sqs_message_handler_registry.find(media_type, daemon.bound_handlers),
        is_(equal_to(noop_handler)),
    )
