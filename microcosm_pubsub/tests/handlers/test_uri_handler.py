from unittest.mock import MagicMock

from hamcrest import assert_that, equal_to, instance_of, is_

from microcosm_pubsub.handlers import URIHandler


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
