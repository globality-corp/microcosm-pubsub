"""
Publish decorator tests.

"""
from json import loads
from hamcrest import (
    assert_that,
    calling,
    equal_to,
    is_,
    raises,
)
from marshmallow import ValidationError
from microcosm.api import create_object_graph

from microcosm_flask.operations import Operation
from microcosm_flask.namespaces import Namespace
from microcosm_pubsub.conventions import changed

from microcosm_pubsub.tests.fixtures import CompanyController, DuckTypeSchema
from microcosm_pubsub.decorators import publish


def create_decorated_controller(graph, decorator):
    class DecoratedCompanyController(CompanyController):
        @decorator
        def retrieve(self):
            return super().retrieve()
    return DecoratedCompanyController(graph)


class TestPublishDecorator:

    def setup(self):
        def loader(metadata):
            return dict(
                sns_topic_arns=dict(
                    default="default",
                )
            )
        self.graph = create_object_graph("example", testing=True, loader=loader)
        self.graph.use(
            "configure_company_v1",
            "configure_user_v1",
            "sns_producer",
        )

        self.graph.lock()
        self.graph.flask.test_client()
        self.client = self.graph.flask.test_client()

    def test_publish(self):
        decorator = publish()
        controller = create_decorated_controller(self.graph, decorator)
        with self.graph.app.test_request_context():
            controller.retrieve()
        assert_that(self.graph.sns_producer.sns_client.publish.call_count, is_(equal_to(1)))
        assert_that(self.graph.sns_producer.sns_client.publish.call_args[1]["TopicArn"], is_(equal_to("default")))
        assert_that(loads(self.graph.sns_producer.sns_client.publish.call_args[1]["Message"]), is_(equal_to({
            "mediaType": "application/vnd.globality.pubsub._.created.company",
            "uri": "http://localhost/api/v1/company/ID",
            "opaqueData": {},
        })))

    def test_set_media_type(self):
        decorator = publish(media_type=changed("something"))
        with self.graph.app.test_request_context():
            create_decorated_controller(self.graph, decorator).retrieve()
        assert_that(loads(self.graph.sns_producer.sns_client.publish.call_args[1]["Message"]), is_(equal_to({
            "mediaType": "application/vnd.globality.pubsub._.changed.something",
            "uri": "http://localhost/api/v1/company/ID",
            "opaqueData": {},
        })))

    def test_set_uri_string_args(self):
        decorator = publish(uri_string_args={
            "name": lambda model: model.name,
            "limit": lambda model: 1,
            "company_id": lambda model: model.id,
        })
        with self.graph.app.test_request_context():
            create_decorated_controller(self.graph, decorator).retrieve()
        assert_that(loads(self.graph.sns_producer.sns_client.publish.call_args[1]["Message"]), is_(equal_to({
            "mediaType": "application/vnd.globality.pubsub._.created.company",
            "uri": "http://localhost/api/v1/company/ID?name=Name&limit=1",
            "opaqueData": {},
        })))

    def test_no_uri(self):
        decorator = publish(default_uri=False)
        create_decorated_controller(self.graph, decorator).retrieve()
        assert_that(loads(self.graph.sns_producer.sns_client.publish.call_args[1]["Message"]), is_(equal_to({
            "mediaType": "application/vnd.globality.pubsub._.created.company",
            "opaqueData": {},
        })))

    def test_custom_uri(self):
        decorator = publish(default_uri=False, message_params={"uri": lambda model: f"http://example.com/{model.id}"})
        create_decorated_controller(self.graph, decorator).retrieve()
        assert_that(loads(self.graph.sns_producer.sns_client.publish.call_args[1]["Message"]), is_(equal_to({
            "mediaType": "application/vnd.globality.pubsub._.created.company",
            "uri": "http://example.com/ID",
            "opaqueData": {},
        })))

    def test_set_message_params(self):
        decorator = publish(media_type=DuckTypeSchema.MEDIA_TYPE, message_params={"quack": lambda model: model.name})
        with self.graph.app.test_request_context():
            create_decorated_controller(self.graph, decorator).retrieve()
        assert_that(loads(self.graph.sns_producer.sns_client.publish.call_args[1]["Message"]), is_(equal_to({
            "quack": "Name",
        })))

    def test_set_operation(self):
        decorator = publish(operation=Operation.Search)
        with self.graph.app.test_request_context():
            create_decorated_controller(self.graph, decorator).retrieve()
        assert_that(loads(self.graph.sns_producer.sns_client.publish.call_args[1]["Message"]), is_(equal_to({
            "mediaType": "application/vnd.globality.pubsub._.created.company",
            "uri": "http://localhost/api/v1/company?company_id=ID",
            "opaqueData": {},
        })))

    def test_set_operation_and_uri_string_args(self):
        decorator = publish(operation=Operation.Search, uri_string_args={})
        with self.graph.app.test_request_context():
            create_decorated_controller(self.graph, decorator).retrieve()
        assert_that(loads(self.graph.sns_producer.sns_client.publish.call_args[1]["Message"]), is_(equal_to({
            "mediaType": "application/vnd.globality.pubsub._.created.company",
            "uri": "http://localhost/api/v1/company",
            "opaqueData": {},
        })))

    def test_missing_producer_key(self):
        decorator = publish()
        controller = create_decorated_controller(self.graph, decorator)
        controller.sns_producer = None
        with self.graph.app.test_request_context():
            assert_that(calling(controller.retrieve), raises(AttributeError))

    def test_get_producer_key_from_graph(self):
        decorator = publish()
        controller = create_decorated_controller(self.graph, decorator)
        controller.sns_producer = None
        setattr(controller, "graph", self.graph)
        with self.graph.app.test_request_context():
            controller.retrieve()
        assert_that(self.graph.sns_producer.sns_client.publish.call_count, is_(equal_to(1)))

    def test_set_producer_key(self):
        decorator = publish(producer_key="sns_producer2")
        controller = create_decorated_controller(self.graph, decorator)
        controller.sns_producer = None
        setattr(controller, "sns_producer2", self.graph.sns_producer)
        with self.graph.app.test_request_context():
            controller.retrieve()
        assert_that(self.graph.sns_producer.sns_client.publish.call_count, is_(equal_to(1)))

    def test_missing_identifier_key(self):
        decorator = publish()
        controller = create_decorated_controller(self.graph, decorator)
        controller.identifier_key = None
        with self.graph.app.test_request_context():
            assert_that(calling(controller.retrieve), raises(ValidationError))

    def test_set_identifier_key(self):
        decorator = publish(identifier_key="company_id")
        controller = create_decorated_controller(self.graph, decorator)
        controller.identifier_key = None

        with self.graph.app.test_request_context():
            controller.retrieve()
        assert_that(loads(self.graph.sns_producer.sns_client.publish.call_args[1]["Message"]), is_(equal_to({
            "mediaType": "application/vnd.globality.pubsub._.created.company",
            "uri": "http://localhost/api/v1/company/ID",
            "opaqueData": {},
        })))

    def test_set_ns_and_identifier_key(self):
        decorator = publish(ns=Namespace(subject="user", version="v1"), identifier_key="user_id")
        with self.graph.app.test_request_context():
            create_decorated_controller(self.graph, decorator).retrieve()
        assert_that(loads(self.graph.sns_producer.sns_client.publish.call_args[1]["Message"]), is_(equal_to({
            "mediaType": "application/vnd.globality.pubsub._.created.user",
            "uri": "http://localhost/api/v1/user/ID",
            "opaqueData": {},
        })))

    def test_missing_ns(self):
        decorator = publish()
        controller = create_decorated_controller(self.graph, decorator)
        controller.ns = None
        with self.graph.app.test_request_context():
            assert_that(calling(controller.retrieve), raises(ValidationError))

    def test_raises_on_undefined_uri(self):
        decorator = publish(message_params={"uri": lambda model: f"http://example.com/{model.id}"})
        assert_that(calling(create_decorated_controller).with_args(self.graph, decorator), raises(ValidationError))
