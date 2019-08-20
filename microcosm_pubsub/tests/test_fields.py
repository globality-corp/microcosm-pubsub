"""
Test fields

"""
from uuid import UUID

from hamcrest import (
    assert_that,
    calling,
    equal_to,
    is_,
    raises,
)
from marshmallow import Schema, ValidationError

from microcosm_pubsub.fields import UUIDField


UUID_STR = '051263b8-707a-4561-a114-11d6706ca5d5'
# UUID_UUID = UUID(UUID_STR)
INVALID_UUID_STR = 'INVALID_UUID_STRING'


class UUIDSchema(Schema):
    uuid_str = UUIDField()
    uuid_uuid = UUIDField()


def test_uuid_load():
    """
    Can deserialize.
    """
    schema = UUIDSchema()
    result = schema.load({
        "uuid_str": UUID_STR,
        "uuid_uuid": UUID(UUID_STR),
    })

    assert_that(result['uuid_str'], is_(equal_to(UUID_STR)))
    assert_that(result['uuid_uuid'], is_(equal_to(UUID_STR)))


def test_invalid_uuid_load():
    """
    Deserializing of non-uuid formatted value should raise an error.

    """
    schema = UUIDSchema()
    assert_that(
        calling(schema.load).with_args({
            "uuid_str": INVALID_UUID_STR,
            "uuid_uuid": UUID(UUID_STR),
        }),
        raises(
            ValidationError,
        ),
    )


def test_uuid_dump():
    """
    Can serialize.
    """
    schema = UUIDSchema()
    result = schema.dump({
        "uuid_str": UUID_STR,
        "uuid_uuid": UUID(UUID_STR),
    })

    assert_that(result['uuid_str'], is_(equal_to(UUID_STR)))
    assert_that(result['uuid_uuid'], is_(equal_to(UUID_STR)))
