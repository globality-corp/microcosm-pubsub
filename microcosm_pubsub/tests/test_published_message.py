from json import loads
from unittest.mock import ANY, patch

from marshmallow import fields
import microcosm.opaque  # noqa
from hamcrest import (
    assert_that,
    has_entries,
    has_properties,
)
from microcosm.api import create_object_graph

import microcosm_pubsub.published_message  # noqa: F401
from microcosm_pubsub.constants import PUBLISHED_KEY
from microcosm_pubsub.codecs import PubSubMessageSchema, PubSubMessageCodec


class MessageSchema(PubSubMessageSchema):
    name = fields.String()

    MEDIA_TYPE = "foo.bar.media_type"


def test_build_message():
    graph = create_object_graph("example", testing=True)
    graph.use(
        "opaque",
        "published_message_builder",
        "pubsub_message_schema_registry",
    )
    with patch.object(graph.pubsub_message_schema_registry, "find") as mocked_find:
        mocked_find.return_value = PubSubMessageCodec(MessageSchema())
        message_1 = graph.published_message_builder(
            media_type="foo.bar.media_type",
            name="foo",
            uri="http://uri",
        )
        assert_that(
            message_1,
            has_properties(
                media_type="foo.bar.media_type",
                message=ANY,
                opaque_data={PUBLISHED_KEY: ANY}
            ),
        )

        assert_that(
            loads(message_1.message),
            has_entries(
                mediaType="foo.bar.media_type",
                name="foo",
                opaqueData={PUBLISHED_KEY: ANY}
            ),
        )

        message_2 = graph.published_message_builder(
            media_type="foo.bar.media_type",
            name="foo",
            uri="http://uri",
            opaque_data=dict(bar="baz")
        )
        assert_that(
            message_2,
            has_properties(
                media_type="foo.bar.media_type",
                message=ANY,
                opaque_data={PUBLISHED_KEY: ANY, "bar": "baz"},
            ),
        )

        assert_that(
            loads(message_2.message),
            has_entries(
                mediaType="foo.bar.media_type",
                name="foo",
                opaqueData={PUBLISHED_KEY: ANY, "bar": "baz"}
            ),
        )
