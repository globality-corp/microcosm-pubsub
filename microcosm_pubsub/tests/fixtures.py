"""
Test fixtures.

"""
from collections import namedtuple
from marshmallow import fields, Schema

from microcosm.api import binding
from microcosm_flask.conventions.base import EndpointDefinition
from microcosm_flask.conventions.crud import configure_crud
from microcosm_flask.operations import Operation
from microcosm_flask.namespaces import Namespace

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
@handles(created("foo"))
@handles(deleted("foo"))
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


class CompanyController:
    def __init__(self, graph):
        self.sns_producer = graph.sns_producer
        self.ns = Namespace(
            subject="company",
            version="v1",
        )
        self.identifier_key = "company_id"

    def retrieve(sel):
        Company = namedtuple("Company", "id name")
        return Company("ID", "Name")


@binding("configure_company_v1")
def configure_company(graph):
    ns = Namespace(
        subject="company",
        version="v1",
    )
    mappings = {
        Operation.Create: EndpointDefinition(),
        Operation.Retrieve: EndpointDefinition(),
        Operation.Search: EndpointDefinition(),
    }

    configure_crud(graph, ns, mappings)
    return ns


@binding("configure_user_v1")
def configure_user(graph):
    ns = Namespace(
        subject="user",
        version="v1",
    )
    mappings = {
        Operation.Retrieve: EndpointDefinition(),
    }

    configure_crud(graph, ns, mappings)
    return ns
