"""
A single SQS message.

"""
from microcosm_pubsub.constants import TTL_KEY


class SQSMessage:
    """
    SQS message wrapper.

    """
    def __init__(self,
                 consumer,
                 content,
                 media_type,
                 message_id,
                 receipt_handle,
                 topic_arn=None,
                 approximate_receive_count=None,
                 handler=None):
        self.consumer = consumer
        self.content = content
        self.media_type = media_type
        self.message_id = message_id
        self.receipt_handle = receipt_handle
        self.topic_arn = topic_arn
        self.approximate_receive_count = approximate_receive_count
        self.handler = handler

    def ack(self):
        """
        Acknowledge this message was processed successfully.

        """
        self.consumer.ack(self)

    def nack(self, visibility_timeout_seconds=None):
        """
        Acknowledge this message was NOT processed successfully.

        """
        self.consumer.nack(self, visibility_timeout_seconds)

    @property
    def opaque_data(self):
        if not self.content:
            return dict()
        return self.content.get("opaque_data", {})

    @property
    def ttl(self):
        if TTL_KEY not in self.opaque_data:
            return None
        return int(self.opaque_data[TTL_KEY])

    @property
    def uri(self):
        if not self.content:
            return None
        return self.content.get("uri")
