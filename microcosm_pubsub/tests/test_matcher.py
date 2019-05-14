"""
Test customized matcher.

"""
from hamcrest import all_of, assert_that
from microcosm.api import create_object_graph, load_from_dict

from microcosm_pubsub.conventions import created
from microcosm_pubsub.matchers import (
    has_media_type,
    has_uri,
    published,
    published_inanyorder,
    published_nothing,
)


class TestPublishingMatcherWithMockedSNS:

    def setup(self):
        loader = load_from_dict(
            sns_producer=dict(
                # NB: mock the boto SNS client (default)
                mock_sns=True,
            ),
            sns_topic_arns=dict(
                default="topic",
            )
        )
        self.graph = create_object_graph("example", testing=True, loader=loader)
        self.graph.sns_producer.sns_client.reset_mocks()

    def test_publish_no_messages(self):
        assert_that(
            self.graph.sns_producer,
            published_nothing(),
        )

    def test_publish_one_message(self):
        self.graph.sns_producer.produce(created("foo"), data="data")

        assert_that(
            self.graph.sns_producer,
            published(
                has_media_type(created("foo")),
            ),
        )

    def test_publish_two_messages(self):
        self.graph.sns_producer.produce(created("foo"), uri="http://localhost")
        self.graph.sns_producer.produce(created("bar"), uri="http://localhost")

        assert_that(
            self.graph.sns_producer,
            published(
                all_of(
                    has_media_type(created("foo")),
                    has_uri(),
                ),
                all_of(
                    has_media_type(created("bar")),
                    has_uri("http://localhost"),
                ),
            ),
        )

    def test_publish_two_messages_non_strict(self):
        self.graph.sns_producer.produce(created("foo"), uri="http://localhost")
        self.graph.sns_producer.produce(created("bar"), uri="http://localhost")

        assert_that(
            self.graph.sns_producer,
            published_inanyorder(
                all_of(
                    has_media_type(created("bar")),
                    has_uri("http://localhost"),
                ),
                all_of(
                    has_media_type(created("foo")),
                    has_uri(),
                ),
            ),
        )


class TestPublishingMatcherWithoutMockedSNS:

    def setup(self):
        loader = load_from_dict(
            sns_producer=dict(
                # NB: mock the SNS producer itself (non-default, except for daemons)
                mock_sns=False,
            ),
            sns_topic_arns=dict(
                default="topic",
            )
        )
        self.graph = create_object_graph("example", testing=True, loader=loader)
        self.graph.sns_producer.sns_client.reset_mocks()

    def test_publish_no_messages(self):
        assert_that(
            self.graph.sns_producer,
            published_nothing(),
        )

    def test_publish_one_message(self):
        self.graph.sns_producer.produce(created("foo"), uri="http://localhost")

        assert_that(
            self.graph.sns_producer,
            published(
                all_of(
                    has_media_type(created("foo")),
                    has_uri("http://localhost"),
                ),
            ),
        )
