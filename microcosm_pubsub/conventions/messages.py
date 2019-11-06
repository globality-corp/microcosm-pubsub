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
    def __init__(self, media_type=None, **kwargs):
        super().__init__(**kwargs)
        # Left as an optional argument for backwards-compatibility
        if media_type is not None:
            self.MEDIA_TYPE = media_type

    uri = fields.String(required=True)


class ChangedURIMessageSchema(URIMessageSchema):
    """
    Define a baseline message schema that points to the URI of a updated resource, with the updated value.

    By convention, pubsub messages are a reference to something which happened, but changed messages can
    be published (and handled) before the changes are committed to DB. Keeping the changed field on the message
    allows us to retry handling the message in case value doesn't match the one in the message.

    """
    field_name = fields.String()
    new_value = fields.String()


class IdentityMessageSchema(PubSubMessageSchema):
    """
    Define a baseline message schema that points to (the identity of) a resource that experienced a lifecycle change.

    In general, these conventions prefer URI-based messages, but for some lifecycle changes (e.g. deletion),
    a URI may not be available.

    """
    def __init__(self, media_type=None, **kwargs):
        super().__init__(**kwargs)
        # Left as an optional argument for backwards-compatibility
        if media_type is not None:
            self.MEDIA_TYPE = media_type

    id = fields.String(required=True)
