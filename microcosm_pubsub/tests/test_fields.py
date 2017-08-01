"""
Test fields

"""
from uuid import UUID
from hamcrest import (
    assert_that,
    contains,
    equal_to,
    is_,
)
from marshmallow import Schema

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

    assert_that(result.data['uuid_str'], is_(equal_to(UUID_STR)))
    assert_that(result.data['uuid_uuid'], is_(equal_to(UUID_STR)))


def test_invlalid_uuid_load():
    """
    Desirializing of non-uuid formatted value should raise an error.
    """
    schema = UUIDSchema()
    result = schema.load({
        "uuid_str": INVALID_UUID_STR,
        "uuid_uuid": UUID(UUID_STR),
    })

    assert_that(result.errors["uuid_str"], contains('Not a valid UUID.'))


def test_uuid_dump():
    """
    Can serialize.
    """
    schema = UUIDSchema()
    result = schema.dump({
        "uuid_str": UUID_STR,
        "uuid_uuid": UUID(UUID_STR),
    })

    assert_that(result.data['uuid_str'], is_(equal_to(UUID_STR)))
    assert_that(result.data['uuid_uuid'], is_(equal_to(UUID_STR)))
