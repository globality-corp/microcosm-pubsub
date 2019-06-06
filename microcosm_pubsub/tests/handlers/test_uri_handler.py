from unittest.mock import MagicMock, patch

from hamcrest import (
    assert_that,
    calling,
    equal_to,
    instance_of,
    is_,
    not_,
    raises,
)
from microcosm.api import create_object_graph

from microcosm_pubsub.errors import Nack
from microcosm_pubsub.handlers import URIHandler


class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data

    def raise_for_status(self):
        pass


class Baz:

    def __init__(self, foo):
        self.foo = foo


class TestURIHandler:

    def test_convert_resource(self):
        graph = MagicMock()
        handler = URIHandler(graph)

        resource = handler.convert_resource(
            dict(
                foo="bar",
            ),
        )
        assert_that(
            resource,
            is_(equal_to(
                dict(
                    foo="bar",
                ),
            )),
        )

    def test_convert_resource_custom_type(self):
        graph = MagicMock()

        class BazURIHandler(URIHandler):

            @property
            def resource_type(self):
                return Baz

        handler = BazURIHandler(graph)
        resource = handler.convert_resource(
            dict(
                foo="bar",
            ),
        )

        assert_that(
            resource,
            is_(instance_of(Baz)),
        )
        assert_that(
            resource.foo,
            is_(equal_to("bar")),
        )

    def test_convert_resource_custom_callable(self):
        graph = MagicMock()

        class BazURIHandler(URIHandler):

            @property
            def resource_type(self):
                def make(**kwargs):
                    return Baz(**kwargs)
                return make

        handler = BazURIHandler(graph)
        resource = handler.convert_resource(
            dict(
                foo="bar",
            ),
        )

        assert_that(
            resource,
            is_(instance_of(Baz)),
        )
        assert_that(
            resource.foo,
            is_(equal_to("bar")),
        )

    def test_get_resource_empty_headers(self):
        graph = create_object_graph("microcosm")
        graph.use(
            "opaque",
            "sqs_message_context",
        )
        graph.lock()

        uri = "http://localhost"
        message = dict(
            uri=uri,
        )

        with patch("microcosm_pubsub.handlers.uri_handler.get") as mocked_get:
            handler = URIHandler(graph)
            handler.get_resource(message, uri)

        mocked_get.assert_called_with(
            uri,
            headers=dict(),
        )

    def test_get_resource_forward_headers(self):
        graph = create_object_graph("microcosm")
        graph.use(
            "opaque",
            "sqs_message_context",
        )
        graph.lock()

        uri = "http://localhost"
        message = dict(
            uri=uri,
        )

        def func():
            return {
                "X-Request-Id": "request-id",
            }

        with patch("microcosm_pubsub.handlers.uri_handler.get") as mocked_get:
            handler = URIHandler(graph)
            with graph.opaque.initialize(func):
                handler.get_resource(message, uri)

        mocked_get.assert_called_with(
            uri,
            headers={
                "X-Request-Id": "request-id",
            },
        )

    def test_nack_when_404(self):
        graph = create_object_graph("microcosm")
        graph.use(
            "opaque",
            "sqs_message_context",
        )
        graph.lock()

        uri = "http://localhost"
        message = dict(
            uri=uri,
        )

        with patch("microcosm_pubsub.handlers.uri_handler.get") as mocked_get:
            mocked_get.return_value = MockResponse(dict(), 404)
            handler = URIHandler(graph)
            assert_that(
                calling(handler.get_resource).with_args(message, uri),
                raises(Nack),
            )

    def test_nack_when_changed_field_not_equal(self):
        graph = create_object_graph("microcosm")
        graph.use(
            "opaque",
            "sqs_message_context",
        )
        graph.lock()

        uri = "http://localhost"
        message = dict(
            uri=uri,
            field_name="foo",
            new_value="bar",
        )

        with patch("microcosm_pubsub.handlers.uri_handler.get") as mocked_get:
            mocked_get.return_value = MockResponse(dict(foo="baz"), 200)
            handler = URIHandler(graph)
            assert_that(
                calling(handler.get_resource).with_args(message, uri),
                raises(Nack),
            )

    def test_handle_when_changed_field_is_equal(self):
        graph = create_object_graph("microcosm")
        graph.use(
            "opaque",
            "sqs_message_context",
        )
        graph.lock()

        uri = "http://localhost"
        message = dict(
            uri=uri,
            field_name="foo",
            new_value="bar",
        )

        with patch("microcosm_pubsub.handlers.uri_handler.get") as mocked_get:
            mocked_get.return_value = MockResponse(dict(foo="bar"), 200)
            handler = URIHandler(graph)
            assert_that(
                calling(handler.get_resource).with_args(message, uri),
                not_(raises(Nack)),
            )
