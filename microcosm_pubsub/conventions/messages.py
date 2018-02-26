from marshmallow import fields

from microcosm_pubsub.codecs import PubSubMessageSchema


class URIMessageSchema(PubSubMessageSchema):
    """
    Define a baseline message schema that points to (the URI of) a resource that experienced a lifecycle change.

    By convention, pubsub messages should be *references* to something that happened within some other
    source-of-truth (e.g. a CRUD microservice). Because there are inherent race conditions between publishing
    a message and commit a persistent transaction, message consumers are expected to call back to source-of-truth
    (e.g. via HTTP) and fetch the current resource value. In the event that message has not been committed yet,
    the consumer can retry a few times before giving up (e.g. via SQS dead-lettering).

    """
    def __init__(self, media_type, **kwargs):
        super().__init__(**kwargs)
        self.MEDIA_TYPE = media_type

    uri = fields.String(required=True)

    def deserialize_media_type(self, obj):
        return self.MEDIA_TYPE


class IdentityMessageSchema(PubSubMessageSchema):
    """
    Define a baseline message schema that points to (the identity of) a resource that experienced a lifecycle change.

    In general, these conventions prefer URI-based messages, but for some lifecycle changes (e.g. deletion),
    a URI may not be available.

    """
    def __init__(self, media_type, **kwargs):
        super().__init__(**kwargs)
        self.MEDIA_TYPE = media_type

    id = fields.String(required=True)

    def deserialize_media_type(self, obj):
        return self.MEDIA_TYPE
