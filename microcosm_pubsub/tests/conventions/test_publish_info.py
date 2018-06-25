"""
Publish info convention tests.

"""

from hamcrest import (
    assert_that,
    equal_to,
    is_,
)
from json import loads
from microcosm.api import create_object_graph
from microcosm_pubsub.tests.fixtures import DerivedSchema


def test_publish_info():
    """
    Default publish info check returns OK.

    """
    def loader(metadata):
        return dict(
            sns_topic_arns=dict(
                default="topic",
            ),
        )
    graph = create_object_graph(name="example", testing=True, loader=loader)
    graph.use("publish_info_convention")
    graph.use("sns_producer")

    # set up response
    graph.sns_producer.sns_client.publish.return_value = dict(MessageId="message-id")

    # Publish two of the same message (testing to maintain a unique set of pubsub calls)
    graph.sns_producer.produce(
        media_type=DerivedSchema.MEDIA_TYPE,
        data="test-uri"
    )

    graph.sns_producer.produce(
        media_type=DerivedSchema.MEDIA_TYPE,
        data="test-uri"
    )

    client = graph.flask.test_client()

    response = client.get("/api/introspection/publish_info")
    assert_that(response.status_code, is_(equal_to(200)))

    decoded_response = loads(response.data.decode("utf-8"))
    assert_that(
        decoded_response["items"][0]["callFunction"],
        is_(equal_to("test_publish_info")),
    )
    assert_that(
        decoded_response["items"][0]["mediaType"],
        is_(equal_to(DerivedSchema.MEDIA_TYPE)),
    )
    assert_that(
        decoded_response["items"][0]["count"],
        is_(equal_to(2)),
    )
    assert_that(
        decoded_response["count"],
        is_(equal_to(1)),
    )
