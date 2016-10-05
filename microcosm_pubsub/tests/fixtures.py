"""
Test fixtures.

"""
from marshmallow import fields
from microcosm.api import binding

from microcosm_pubsub.codecs import PubSubMessageSchema

FOO_MEDIA_TYPE = "application/vnd.globality.pubsub.foo"
FOO_QUEUE_URL = "foo-queue-url"
FOO_TOPIC = "foo-topic"
MESSAGE_ID = "message-id"
RECEIPT_HANDLE = "receipt-handle"


class FooSchema(PubSubMessageSchema):
    MEDIA_TYPE = FOO_MEDIA_TYPE

    bar = fields.String(required=True)

    def deserialize_media_type(self, obj):
        return FooSchema.MEDIA_TYPE


def foo_handler(message):
    return True


@binding("pubsub_message_codecs")
def configure_pubsub_message_codecs(graph):
    return {
        FooSchema.MEDIA_TYPE: FooSchema,
    }


@binding("sqs_message_handlers")
def configure_sqs_message_handlers(graph):
    return {
        FooSchema.MEDIA_TYPE: foo_handler,
    }
