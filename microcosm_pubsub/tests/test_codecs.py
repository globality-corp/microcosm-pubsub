"""
Message encoding tests.

"""
from json import dumps, loads

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

from microcosm_pubsub.tests.fixtures import DerivedSchema


def test_no_default_schema():
    """
    An unknown message type will fail.

    """
    graph = create_object_graph("example", testing=True)
    assert_that(
        calling(graph.pubsub_message_schema_registry.find).with_args("unknown"),
        raises(KeyError),
    )


def test_custom_schema():
    """
    A configured message type will use its schema.

    """
    graph = create_object_graph("example", testing=True)
    codec = graph.pubsub_message_schema_registry.find(DerivedSchema.MEDIA_TYPE)
    assert_that(codec.schema, is_(instance_of(DerivedSchema)))


def test_encode():
    """
    A message will be encoded according to its schema.

    """
    graph = create_object_graph("example", testing=True)
    codec = graph.pubsub_message_schema_registry.find(DerivedSchema.MEDIA_TYPE)
    assert_that(loads(codec.encode(data="data")), is_(equal_to({
        "data": "data",
        "mediaType": DerivedSchema.MEDIA_TYPE,
    })))


def test_encode_missing_field():
    """
    An invalid message will raise errors.

    """
    graph = create_object_graph("example", testing=True)
    codec = graph.pubsub_message_schema_registry.find(DerivedSchema.MEDIA_TYPE)
    assert_that(calling(codec.encode), raises(ValidationError))


def test_decode():
    """
    A message will be decoded according to its schema.

    """
    graph = create_object_graph("example", testing=True)
    codec = graph.pubsub_message_schema_registry.find(DerivedSchema.MEDIA_TYPE)
    message = dumps({
        "data": "data",
        "mediaType": DerivedSchema.MEDIA_TYPE,
    })
    assert_that(codec.decode(message), is_(equal_to({
        "data": "data",
        "media_type": DerivedSchema.MEDIA_TYPE,
    })))


def test_decode_missing_media_type():
    """
    An invalid message will raise errors.

    """
    graph = create_object_graph("example", testing=True)
    codec = graph.pubsub_message_schema_registry.find(DerivedSchema.MEDIA_TYPE)
    message = dumps({
        "bar": "baz",
    })
    assert_that(calling(codec.decode).with_args(message), raises(ValidationError))


def test_decode_missing_field():
    """
    An invalid message will raise errors.

    """
    graph = create_object_graph("example", testing=True)
    codec = graph.pubsub_message_schema_registry.find(DerivedSchema.MEDIA_TYPE)
    message = dumps({
        "mediaType": DerivedSchema.MEDIA_TYPE,
    })
    assert_that(calling(codec.decode).with_args(message), raises(ValidationError))
