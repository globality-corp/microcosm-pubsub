"""
Publish decorator tests.

"""
from collections import namedtuple
from json import loads
from hamcrest import (
    assert_that,
    calling,
    equal_to,
    is_,
    raises,
)
from microcosm.api import create_object_graph

from microcosm_pubsub.conventions import created
from microcosm_pubsub.tests.fixtures import DuckTypeSchema
from microcosm_pubsub.decorators import publish


def create_controller(graph, decorator):
    class CompanyController:
        def __init__(self, graph):
            self.sns_producer = graph.sns_producer

        @decorator
        def retrieve(self):
            return namedtuple("Company", "id name")("ID", "Name")
    return CompanyController(graph)


class TestPublishDecorator:

    def setup(self):
        def loader(metadata):
            return dict(
                sns_topic_arns=dict(
                    default="default",
                )
            )
        self.graph = create_object_graph("example", testing=True, loader=loader)
        self.graph.use("sns_producer")
        self.graph.lock()

    def test_set_media_type(self):
        decorator = publish(media_type=created("company"))
        create_controller(self.graph, decorator).retrieve()
        assert_that(loads(self.graph.sns_producer.sns_client.publish.call_args[1]["Message"]), is_(equal_to({
            "mediaType": "application/vnd.globality.pubsub._.created.company",
            "opaqueData": {},
        })))

    def test_set_media_type_extractor(self):
        decorator = publish(media_type_extractor=lambda ctrl, model: created(ctrl.subject))
        controller = create_controller(self.graph, decorator)
        setattr(controller, "subject", "company")
        controller.retrieve()
        assert_that(loads(self.graph.sns_producer.sns_client.publish.call_args[1]["Message"]), is_(equal_to({
            "mediaType": "application/vnd.globality.pubsub._.created.company",
            "opaqueData": {},
        })))

    def test_uri_message_params(self):
        decorator = publish(
            media_type=created("company"),
            uri=lambda ctrl, model: f"http://example.com/{model.id}",
            quack=lambda ctrl, model: model.name,
        )
        create_controller(self.graph, decorator).retrieve()
        assert_that(loads(self.graph.sns_producer.sns_client.publish.call_args[1]["Message"]), is_(equal_to({
            "mediaType": "application/vnd.globality.pubsub._.created.company",
            "uri": "http://example.com/ID",
            "opaqueData": {},
        })))

    def test_custom_message_params(self):
        decorator = publish(
            media_type=DuckTypeSchema.MEDIA_TYPE,
            uri=lambda ctrl, model: f"http://example.com/{model.id}",
            quack=lambda ctrl, model: model.name,
        )
        create_controller(self.graph, decorator).retrieve()
        assert_that(loads(self.graph.sns_producer.sns_client.publish.call_args[1]["Message"]), is_(equal_to({
            "quack": "Name",
        })))

    def test_missing_producer_key(self):
        decorator = publish(media_type=created("company"))
        controller = create_controller(self.graph, decorator)
        controller.sns_producer = None
        assert_that(calling(controller.retrieve), raises(AttributeError))

    def test_get_producer_key_from_graph(self):
        decorator = publish(media_type=created("company"))
        controller = create_controller(self.graph, decorator)
        controller.sns_producer = None
        setattr(controller, "graph", self.graph)
        controller.retrieve()
        assert_that(self.graph.sns_producer.sns_client.publish.call_count, is_(equal_to(1)))

    def test_set_producer_key(self):
        decorator = publish(media_type=created("company"), producer_key="sns_producer2")
        controller = create_controller(self.graph, decorator)
        controller.sns_producer = None
        setattr(controller, "sns_producer2", self.graph.sns_producer)
        controller.retrieve()
        assert_that(self.graph.sns_producer.sns_client.publish.call_count, is_(equal_to(1)))

    def test_missing_media_type(self):
        assert_that(calling(publish), raises(TypeError))

    def test_missing_dual_media_type_keys(self):
        assert_that(calling(publish).with_args(
            media_type=created("company"),
            media_type_extractor=lambda ctrl, model: created(ctrl.subject),
        ), raises(TypeError))
