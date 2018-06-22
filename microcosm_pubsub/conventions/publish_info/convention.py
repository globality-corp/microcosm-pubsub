"""
Publish Info convention.

"""
from microcosm_flask.conventions.base import EndpointDefinition
from microcosm_flask.conventions.crud import configure_crud
from microcosm_flask.operations import Operation
from microcosm_flask.namespaces import Namespace
from microcosm_pubsub.conventions.publish_info.resources import (
    PublishInfoSchema,
    Schema,
)


def configure_publish_info(graph):
    ns = Namespace(
        subject="introspection/publish_info",
    )

    def search(**kwargs):
        publish_info = list(graph.sns_producer.get_publish_info())
        return publish_info, len(publish_info)

    mappings = {
        Operation.Search: EndpointDefinition(
            func=search,
            request_schema=Schema(),
            response_schema=PublishInfoSchema(),
        ),
    }

    configure_crud(graph, ns, mappings)
    return ns
