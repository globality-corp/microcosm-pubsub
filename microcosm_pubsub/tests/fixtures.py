"""
Test fixtures.

"""
from marshmallow import fields, Schema

from microcosm_pubsub.codecs import PubSubMessageSchema
from microcosm_pubsub.conventions import created
from microcosm_pubsub.daemon import ConsumerDaemon
from microcosm_pubsub.decorators import handles, schema
from microcosm_pubsub.errors import SkipMessage


FOO_MEDIA_TYPE = "application/vnd.globality.pubsub.foo"
FOO_QUEUE_URL = "foo-queue-url"
FOO_TOPIC = "foo-topic"
MESSAGE_ID = "message-id"
RECEIPT_HANDLE = "receipt-handle"


class ExampleDaemon(ConsumerDaemon):

    @property
    def name(self):
        return "example"


class Foo(object):
    pass


@schema
class FooSchema(PubSubMessageSchema):
    """
    Example schema explicitly deriving from `PubSubMessageSchema`

    """
    MEDIA_TYPE = FOO_MEDIA_TYPE

    bar = fields.String(required=True)

    def deserialize_media_type(self, obj):
        return FooSchema.MEDIA_TYPE


@schema
class TestSchema(Schema):
    """
    Example schema from scratch.

    """
    MEDIA_TYPE = "test"

    test = fields.String()


@handles(TestSchema)
@handles(created("foo"))
def noop_handler(message):
    return True


@handles(created("bar"))
def skipping_handler(message):
    raise SkipMessage("Failed")


@handles(FooSchema.MEDIA_TYPE)
def foo_handler(message):
    return True
