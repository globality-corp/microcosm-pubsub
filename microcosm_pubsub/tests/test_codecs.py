"""
Codec tests.

"""
from json import dumps

from hamcrest import (
    assert_that,
    calling,
    equal_to,
    instance_of,
    is_,
    raises,
)
from marshmallow import ValidationError
from microcosm.api import create_object_graph

from microcosm_pubsub.codecs import PubSubMessageSchema
from microcosm_pubsub.tests.fixtures import FooSchema


def test_default_schema():
    """
    An unknown message type will use the default schema.

    """
    def loader(metadata):
        return dict(
            pubsub_message_codecs=dict(
                default=FooSchema,
            ),
        )

    graph = create_object_graph("example", testing=True, loader=loader)
    codec = graph.pubsub_message_codecs["foo"]
    assert_that(codec.schema, is_(instance_of(FooSchema)))


def test_custom_schema():
    """
    A configured message type will use its schema.

    """
    def loader(metadata):
        return dict(
            pubsub_message_codecs=dict(
                default=PubSubMessageSchema,
                mappings=dict(
                    foo=FooSchema,
                ),
            ),
        )

    graph = create_object_graph("example", testing=True, loader=loader)
    codec = graph.pubsub_message_codecs["foo"]
    assert_that(codec.schema, is_(instance_of(FooSchema)))


def test_encode():
    """
    A message will be encoded according to its schema.

    """
    def loader(metadata):
        return dict(
            pubsub_message_codecs=dict(
                default=FooSchema,
            ),
        )

    graph = create_object_graph("example", testing=True, loader=loader)
    codec = graph.pubsub_message_codecs["foo"]
    assert_that(codec.encode(bar="baz"), is_(equal_to(dumps({
        "bar": "baz",
        "mediaType": "application/vnd.globality.pubsub.foo",
    }))))


def test_encode_missing_field():
    """
    An invalid message will raise errors.

    """
    def loader(metadata):
        return dict(
            pubsub_message_codecs=dict(
                default=FooSchema,
            ),
        )

    graph = create_object_graph("example", testing=True, loader=loader)
    codec = graph.pubsub_message_codecs["foo"]
    assert_that(calling(codec.encode).with_args(baz="bar"), raises(ValidationError))


def test_decode():
    """
    A message will be decoded according to its schema.

    """
    def loader(metadata):
        return dict(
            pubsub_message_codecs=dict(
                default=FooSchema,
            ),
        )

    graph = create_object_graph("example", testing=True, loader=loader)
    codec = graph.pubsub_message_codecs["foo"]
    message = dumps({
        "bar": "baz",
        "mediaType": "application/vnd.globality.pubsub.foo",
    })
    assert_that(codec.decode(message), is_(equal_to({
        "bar": "baz",
        "media_type": "application/vnd.globality.pubsub.foo",
    })))


def test_decode_missing_media_type():
    """
    An invalid message will raise errors.

    """
    def loader(metadata):
        return dict(
            pubsub_message_codecs=dict(
                default=FooSchema,
            ),
        )

    graph = create_object_graph("example", testing=True, loader=loader)
    codec = graph.pubsub_message_codecs["foo"]
    message = dumps({
        "bar": "baz",
    })
    assert_that(calling(codec.decode).with_args(message), raises(ValidationError))


def test_decode_missing_field():
    """
    An invalid message will raise errors.

    """
    def loader(metadata):
        return dict(
            pubsub_message_codecs=dict(
                default=FooSchema,
            ),
        )

    graph = create_object_graph("example", testing=True, loader=loader)
    codec = graph.pubsub_message_codecs["foo"]
    message = dumps({
        "mediaType": "application/vnd.globality.pubsub.foo",
    })
    assert_that(calling(codec.decode).with_args(message), raises(ValidationError))
