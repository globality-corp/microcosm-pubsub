"""
URI and Identity message tests.

"""
from enum import Enum, auto
from json import dumps, loads
from time import time
from unittest.mock import patch

from hamcrest import (
    assert_that,
    equal_to,
    instance_of,
    is_,
)
from marshmallow.fields import Field
from microcosm.api import create_object_graph

from microcosm_pubsub.codecs import PubSubMessageCodec
from microcosm_pubsub.conventions import (
    IdentityMessageSchema,
    URIMessageSchema,
    created,
    deleted,
)
from microcosm_pubsub.decorators import schema
from microcosm_pubsub.tests.fixtures import ExampleDaemon, noop_handler


class Foo:
    pass


class TestEnum(Enum):
    key = auto()

    def __str__(self):
        return self.name


class EnumField(Field):
    """
    Test serialization of non-serializable fields
    This is mostly taken from microcosm-flask's EnumField

    """
    default_error_messages = {
        "by_name": "Invalid enum member {name}",
    }

    def __init__(self, enum, **kwargs):
        super().__init__(**kwargs)
        self.enum = enum

    def _serialize(self, value, attr, obj, **kwargs):
        if value is None:
            return value
        elif isinstance(value, str) and not isinstance(value, Enum):
            return value
        else:
            return value.name

    def _deserialize(self, value, attr, data, **kwargs):
        if value is None:
            return value
        else:
            return self._deserialize_by_name(value)

    def _deserialize_by_name(self, value):
        try:
            return getattr(self.enum, value)
        except AttributeError:
            self.fail("by_name", name=value)


@schema
class CustomMessageSchema(URIMessageSchema):
    """
    Message indicating that a resource was created

    """
    MEDIA_TYPE = created("Resource")
    enumField = EnumField(TestEnum, attribute="enum_field", required=True)


def test_encode_uri_message_schema():
    """
    Message encoding should include the standard URIMessage fields.

    """
    schema = URIMessageSchema()
    codec = PubSubMessageCodec(schema)
    assert_that(
        loads(codec.encode(
            opaque_data=dict(foo="bar"),
            uri="http://example.com",
            media_type="application/vnd.globality.pubsub._.deleted.foo",
        )),
        is_(equal_to({
            "mediaType": "application/vnd.globality.pubsub._.deleted.foo",
            "opaqueData": {
                "foo": "bar",
            },
            "uri": "http://example.com",
        })),
    )


def test_encode_custom_message():
    """
    Custom message encoding should include the standard URIMessage fields plus additionally
    specified fields

    """
    custom_schema = CustomMessageSchema()
    codec = PubSubMessageCodec(custom_schema)
    assert_that(
        loads(codec.encode(
            enum_field=TestEnum.key,
            media_type=CustomMessageSchema.MEDIA_TYPE,
            opaque_data=dict(foo="bar"),
            uri="http://example.com",
        )),
        is_(equal_to({
            "mediaType": "application/vnd.globality.pubsub._.created.resource",
            "opaqueData": {
                "foo": "bar",
            },
            "uri": "http://example.com",
            "enumField": "key",
        })),
    )


def test_encode_identity_message_schema():
    """
    Message encoding should include the standard IdentityMessage fields.

    """
    schema = IdentityMessageSchema()
    codec = PubSubMessageCodec(schema)
    assert_that(
        loads(codec.encode(
            opaque_data=dict(foo="bar"),
            media_type="application/vnd.globality.pubsub._.deleted.foo",
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
    Message decoding should process standard URIMessage fields.

    """
    schema = URIMessageSchema()
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
    Message decoding should process standard IdentityMessage fields.

    """
    identity_schema = IdentityMessageSchema()
    codec = PubSubMessageCodec(identity_schema)
    message = dumps({
        "mediaType": "application/vnd.globality.pubsub._.created.foo",
        "opaqueData": {
            "foo": "bar",
        },
        "id": "1",
    })
    assert_that(codec.decode(message), is_(equal_to({
        "media_type": "application/vnd.globality.pubsub._.created.foo",
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

    published_time = str(time())
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
    assert_that(graph.sns_producer.sns_client.publish.call_args[1]["MessageAttributes"], is_(equal_to({
        "media_type": {
            "DataType": "String",
            "StringValue": "application/vnd.globality.pubsub._.created.foo"
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
            "X-Request-Published": str(published_time),
        },
    })))
    assert_that(graph.sns_producer.sns_client.publish.call_args[1]["MessageAttributes"], is_(equal_to({
        "media_type": {
            "DataType": "String",
            "StringValue": "application/vnd.globality.pubsub._.deleted.foo"
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
