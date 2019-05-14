"""
Test fixtures.

"""
from marshmallow import Schema, fields
from microcosm.api import binding

from microcosm_pubsub.codecs import PubSubMessageSchema
from microcosm_pubsub.conventions import created, deleted
from microcosm_pubsub.daemon import ConsumerDaemon
from microcosm_pubsub.decorators import handles, schema
from microcosm_pubsub.errors import SkipMessage


@schema
class DerivedSchema(PubSubMessageSchema):
    """
    A schema that is derived from `PubSubMessageSchema`

    """
    MEDIA_TYPE = "application/vnd.microcosm.derived"

    data = fields.String(required=True)

    def deserialize_media_type(self, obj):
        return DerivedSchema.MEDIA_TYPE


@schema
class DuckTypeSchema(Schema):
    """
    A duck typed schema

    """
    MEDIA_TYPE = "application/vnd.microcosm.duck"

    quack = fields.String()


@handles(DuckTypeSchema)
@handles(created("Foo"))
@handles(deleted("Foo"))
@handles(DerivedSchema.MEDIA_TYPE)
def noop_handler(message):
    return True


@handles(created("IgnoredResource"))
def skipping_handler(message):
    raise SkipMessage("Failed")


class ExampleDaemon(ConsumerDaemon):

    @property
    def name(self):
        return "example"

    @property
    def components(self):
        return super().components + [
            "noop_handler",
        ]


@binding("noop_handler")
def configure_noop_handler(graph):
    return noop_handler
