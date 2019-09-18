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
from microcosm.loaders import load_from_dict
from nose.plugins.attrib import attr

from microcosm_pubsub.constants import DEFAULT_RESOURCE_CACHE_TTL
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

    @attr("caching")
    def test_get_resource_with_cache_disabled_on_instance(self):
        config = dict(
            resource_cache=dict(
                enabled=True,
            ),
        )
        graph = create_object_graph("microcosm", testing=True, loader=load_from_dict(config))
        graph.use(
            "opaque",
            "resource_cache",
            "sqs_message_context",
        )
        graph.lock()

        uri = "http://localhost"
        message = dict(
            uri=uri,
        )
        json_data = dict(foo="bar", bar="baz")

        with patch("microcosm_pubsub.handlers.uri_handler.get") as mocked_get:
            mocked_get.return_value = MockResponse(
                status_code=200,
                json_data=json_data,
            )
            handler = URIHandler(graph, resource_cache_enabled=False)
            handler.get_resource(message, uri)

        assert_that(handler.resource_cache, is_(None))

        mocked_get.assert_called_with(
            uri,
            headers=dict(),
        )

    @attr("caching")
    def test_get_non_whitelisted_resource_with_cache_enabled(self):
        config = dict(
            resource_cache=dict(
                enabled=True,
            ),
        )
        graph = create_object_graph("microcosm", testing=True, loader=load_from_dict(config))
        graph.use(
            "opaque",
            "resource_cache",
            "sqs_message_context",
        )
        graph.lock()

        # Nb. changed events should not be whitelisted for caching
        uri = "https://service.env.globality.io/api/v2/project_event/0598355c-5b19-49bd-a755-146204220a5b"
        media_type = "application/vnd.globality.pubsub._.changed.project_event.project_brief_submitted"
        message = dict(
            uri=uri,
            mediaType=media_type,
        )
        json_data = dict(foo="bar", bar="baz")

        with patch("microcosm_pubsub.handlers.uri_handler.get") as mocked_get:
            mocked_get.return_value = MockResponse(
                status_code=200,
                json_data=json_data,
            )
            handler = URIHandler(graph)
            with patch.object(handler.resource_cache, "get") as mocked_cache_get:
                mocked_cache_get.return_value = None
                with patch.object(handler.resource_cache, "set") as mocked_cache_set:
                    handler.get_resource(message, uri)

        mocked_get.assert_called_with(
            uri,
            headers=dict(),
        )

        # Nb. cache get/set were not called due to resource uri not being whitelisted
        assert_that(mocked_cache_get.called, is_(equal_to(False)))
        assert_that(mocked_cache_set.called, is_(equal_to(False)))

    @attr("caching")
    def test_get_whitelisted_resource_with_cache_enabled_and_cache_miss(self):
        config = dict(
            resource_cache=dict(
                enabled=True,
            ),
        )
        graph = create_object_graph("microcosm", testing=True, loader=load_from_dict(config))
        graph.use(
            "opaque",
            "resource_cache",
            "sqs_message_context",
        )
        graph.lock()

        uri = "https://service.env.globality.io/api/v2/project_event/0598355c-5b19-49bd-a755-146204220a5b"
        media_type = "application/vnd.globality.pubsub._.created.project_event.project_brief_submitted"
        message = dict(
            uri=uri,
            mediaType=media_type,
        )
        json_data = dict(foo="bar", bar="baz")

        with patch("microcosm_pubsub.handlers.uri_handler.get") as mocked_get:
            mocked_get.return_value = MockResponse(
                status_code=200,
                json_data=json_data,
            )
            handler = URIHandler(graph)

            with patch.object(handler.resource_cache, "get") as mocked_cache_get:
                mocked_cache_get.return_value = None
                with patch.object(handler.resource_cache, "set") as mocked_cache_set:
                    handler.get_resource(message, uri)

        mocked_get.assert_called_with(
            uri,
            headers=dict(),
        )
        # Nb. cache get was attempted
        mocked_cache_get.assert_called_with(
            uri,
        )
        # Nb. cache set was called due to cache miss
        mocked_cache_set.assert_called_with(
            uri,
            json_data,
            ttl=DEFAULT_RESOURCE_CACHE_TTL,
        )

    @attr("caching")
    def test_get_whitelisted_resource_with_cache_enabled_and_cache_miss_and_custom_ttl(self):
        config = dict(
            resource_cache=dict(
                enabled=True,
            ),
        )
        graph = create_object_graph("microcosm", testing=True, loader=load_from_dict(config))
        graph.use(
            "opaque",
            "resource_cache",
            "sqs_message_context",
        )
        graph.lock()

        uri = "https://service.env.globality.io/api/v2/project_event/0598355c-5b19-49bd-a755-146204220a5b"
        media_type = "application/vnd.globality.pubsub._.created.project_event.project_brief_submitted"
        message = dict(
            uri=uri,
            mediaType=media_type,
        )
        json_data = dict(foo="bar", bar="baz")

        with patch("microcosm_pubsub.handlers.uri_handler.get") as mocked_get:
            mocked_get.return_value = MockResponse(
                status_code=200,
                json_data=json_data,
            )
            handler = URIHandler(graph, resource_cache_ttl=100)

            with patch.object(handler.resource_cache, "get") as mocked_cache_get:
                mocked_cache_get.return_value = None
                with patch.object(handler.resource_cache, "set") as mocked_cache_set:
                    handler.get_resource(message, uri)

        mocked_get.assert_called_with(
            uri,
            headers=dict(),
        )
        # Nb. cache get was attempted
        mocked_cache_get.assert_called_with(
            uri,
        )
        # Nb. cache set was called due to cache miss
        mocked_cache_set.assert_called_with(
            uri,
            json_data,
            ttl=100,
        )

    @attr("caching")
    def test_get_whitelisted_resource_with_cache_enabled_and_cache_hit(self):
        config = dict(
            resource_cache=dict(
                enabled=True,
            ),
        )
        graph = create_object_graph("microcosm", testing=True, loader=load_from_dict(config))
        graph.use(
            "opaque",
            "resource_cache",
            "sqs_message_context",
        )
        graph.lock()

        uri = "https://service.env.globality.io/api/v2/project_event/0598355c-5b19-49bd-a755-146204220a5b"
        media_type = "application/vnd.globality.pubsub._.created.project_event.project_brief_submitted"
        message = dict(
            uri=uri,
            mediaType=media_type,
        )
        json_data = dict(foo="bar", bar="baz")

        with patch("microcosm_pubsub.handlers.uri_handler.get") as mocked_get:
            mocked_get.return_value = MockResponse(
                status_code=200,
                json_data=json_data,
            )
            handler = URIHandler(graph)

            with patch.object(handler.resource_cache, "get") as mocked_cache_get:
                mocked_cache_get.return_value = json_data
                with patch.object(handler.resource_cache, "set") as mocked_cache_set:
                    handler.get_resource(message, uri)

        # Nb. cache get was attempted
        mocked_cache_get.assert_called_with(
            uri,
        )
        # Nb. actual resource HTTP retrieve not called since cache was hit
        assert_that(mocked_get.called, is_(equal_to(False)))

        # Nb. cache set was not called due to cache hit
        assert_that(mocked_cache_set.called, is_(equal_to(False)))

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
