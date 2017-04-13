"""
Test fixtures.

"""
from marshmallow import fields

from microcosm_pubsub.codecs import PubSubMessageSchema
from microcosm_pubsub.daemon import ConsumerDaemon
from microcosm_pubsub.decorators import handles, schema


FOO_MEDIA_TYPE = "application/vnd.globality.pubsub.foo"
FOO_QUEUE_URL = "foo-queue-url"
FOO_TOPIC = "foo-topic"
MESSAGE_ID = "message-id"
RECEIPT_HANDLE = "receipt-handle"


class ExampleDaemon(ConsumerDaemon):

    @property
    def name(self):
        return "example"


@schema
class FooSchema(PubSubMessageSchema):
    MEDIA_TYPE = FOO_MEDIA_TYPE

    bar = fields.String(required=True)

    def deserialize_media_type(self, obj):
        return FooSchema.MEDIA_TYPE


@handles(FooSchema.MEDIA_TYPE)
def foo_handler(message):
    return True
