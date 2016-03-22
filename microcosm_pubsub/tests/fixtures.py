"""
Test fixtures.

"""
from marshmallow import fields

from microcosm_pubsub.codecs import PubSubMessageSchema

FOO_MEDIA_TYPE = "application/vnd.globality.pubsub.foo"
FOO_QUEUE_URL = "foo-queue-url"
FOO_TOPIC = "foo-topic"
MESSAGE_ID = "message-id"
RECEIPT_HANDLE = "receipt-handle"


class FooSchema(PubSubMessageSchema):
    bar = fields.String(required=True)

    def deserialize_media_type(self, obj):
        return FOO_MEDIA_TYPE
