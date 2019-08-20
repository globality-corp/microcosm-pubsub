"""
Custom fields.

"""
from uuid import UUID

from marshmallow.fields import Field, ValidationError


class UUIDField(Field):
    """
    UUID valued field, as either a string or UUID object.

    """
    def _serialize(self, value, attr, obj, **kwargs):
        """
        Serialize value as a uuid, either as a string or an UUID object.
        """
        if value is None:
            return None

        if isinstance(value, UUID):
            return str(value)
        else:
            return value

    def _deserialize(self, value, attr, data, **kwargs):
        """
        Deserialize value as a uuid.

        Handle both string and UUID object.
        """
        if value is None:
            return None

        try:
            if isinstance(value, UUID):
                return str(value)
            else:
                return str(UUID(value))

        except ValueError:
            raise ValidationError('Not a valid UUID.')
