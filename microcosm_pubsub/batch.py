from marshmallow import Schema, fields

from microcosm_pubsub.codecs import PubSubMessageSchema
from microcosm_pubsub.conventions import created
from microcosm_pubsub.decorators import schema


class BatchedMessageSchema(Schema):
    """
    A wrapper for a single message that is to be published in a
    MessageBatchSchema.

    """
    media_type = fields.String(required=True)
    message = fields.Raw(required=True)
    topic_arn = fields.String(required=True)
    opaque_data = fields.Raw(required=True)


@schema
class MessageBatchSchema(PubSubMessageSchema):
    """
    A message indicating that a batch of messages needs to be published.

    """
    MEDIA_TYPE = created("batch_message")

    messages = fields.List(fields.Nested(BatchedMessageSchema), required=True)

    def deserialize_media_type(self, obj):
        return MessageBatchSchema.MEDIA_TYPE
