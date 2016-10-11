"""
Convention tests.

"""
from json import dumps, loads

from hamcrest import (
    assert_that,
    equal_to,
    instance_of,
    is_
)
from microcosm.api import create_object_graph

from microcosm_pubsub.codecs import PubSubMessageCodec
from microcosm_pubsub.conventions import created, make_media_type, URIMessageSchema
from microcosm_pubsub.decorators import handles


class Foo(object):
    pass


@handles(created("foo"))
def noop_handler(message):
    return True


def test_make_media_type():
    """
    Media type construction should generate the correct type strings.

    """
    cases = [
        (("foo",), dict(), "application/vnd.globality.pubsub._.created.foo"),
        (("foo",), dict(public=True), "application/vnd.globality.pubsub.created.foo"),
        (("foo",), dict(organization="example"), "application/vnd.example.pubsub._.created.foo"),
        (("FooBar",), dict(), "application/vnd.globality.pubsub._.created.foo_bar"),
        (("FooBar.ThisThat",), dict(), "application/vnd.globality.pubsub._.created.foo_bar.this_that"),
    ]

    def validate(args, kargs, expected):
        assert_that(make_media_type(*args, **kwargs), is_(equal_to(expected)))

    for args, kwargs, expected in cases:
        yield validate, args, kwargs, expected


def test_encode_uri_message_schema():
    """
    Message encoding should include the standard fields.

    """
    schema = URIMessageSchema(make_media_type("Foo"))
    codec = PubSubMessageCodec(schema)
    assert_that(
        loads(codec.encode(
            opaque_data=dict(foo="bar"),
            uri="http://example.com",
        )),
        is_(equal_to({
            "mediaType": "application/vnd.globality.pubsub._.created.foo",
            "opaqueData": {
                "foo": "bar",
            },
            "uri": "http://example.com",
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


def test_publish_by_convention():
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

    graph.sns_producer.produce(created("foo"), uri="http://example.com", opaque_data=dict())

    assert_that(graph.sns_producer.sns_client.publish.call_count, is_(equal_to(1)))
    assert_that(graph.sns_producer.sns_client.publish.call_args[1]["TopicArn"], is_(equal_to("default")))
    assert_that(loads(graph.sns_producer.sns_client.publish.call_args[1]["Message"]), is_(equal_to({
        "mediaType": "application/vnd.globality.pubsub._.created.foo",
        "uri": "http://example.com",
        "opaqueData": {},
    })))


def test_dispatch_by_convention():
    """
    Message dispatch can use this convention.

    """
    graph = create_object_graph("example", testing=True)

    media_type = created(Foo)

    assert_that(
        graph.pubsub_message_schema_registry[media_type].schema,
        is_(instance_of(URIMessageSchema)),
    )

    assert_that(
        graph.sqs_message_handler_registry[media_type],
        is_(equal_to(noop_handler)),
    )
