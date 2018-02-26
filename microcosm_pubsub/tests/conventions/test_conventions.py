"""
Test conventions.

"""
from hamcrest import assert_that, equal_to, instance_of, is_

from microcosm.api import create_object_graph
from microcosm_pubsub.conventions import created, deleted, media_type
from microcosm_pubsub.conventions.messages import IdentityMessageSchema, URIMessageSchema


def borked(resource, **kwargs):
    return media_type("borked")(resource, **kwargs)


class TestConventions:

    def setup(self):
        self.graph = create_object_graph("test")
        self.graph.use(
            "pubsub_message_schema_registry",
            "pubsub_lifecycle_change",
        )
        self.graph.lock()

    def test_created(self):
        assert_that(
            created("Foo.Bar"),
            is_(equal_to("application/vnd.globality.pubsub._.created.foo.bar")),
        )

        assert_that(
            self.graph.pubsub_message_schema_registry.find(
                "application/vnd.globality.pubsub._.created.foo.bar",
            ).schema,
            is_(instance_of(URIMessageSchema)),
        )

    def test_deleted(self):
        assert_that(
            deleted("Foo.Bar"),
            is_(equal_to("application/vnd.globality.pubsub._.deleted.foo.bar")),
        )

        assert_that(
            self.graph.pubsub_message_schema_registry.find(
                "application/vnd.globality.pubsub._.deleted.foo.bar",
            ).schema,
            is_(instance_of(IdentityMessageSchema)),
        )

    def test_custom_lifecycle_change(self):
        self.graph.pubsub_lifecycle_change.add("borked")

        assert_that(
            borked("Foo.Bar"),
            is_(equal_to("application/vnd.globality.pubsub._.borked.foo.bar")),
        )

        assert_that(
            self.graph.pubsub_message_schema_registry.find(
                "application/vnd.globality.pubsub._.borked.foo.bar",
            ).schema,
            is_(instance_of(URIMessageSchema)),
        )
