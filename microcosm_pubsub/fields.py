"""
Custom fields.
"""
from marshmallow.fields import Field, ValidationError
from uuid import UUID


class UUIDField(Field):
    """
    UUID valued field, as either a string or UUID object.

    """
    def __init__(self, use_uuidformat=False, *args, **kwargs):
        self.use_uuidformat = use_uuidformat
        super(UUIDField, self).__init__(*args, **kwargs)

    def _serialize(self, value, attr, obj):
        """
        Serialize value as a uuid, either as a string or an UUID object.
        """
        if value is None:
            return None

        if self.use_uuidformat:
            return str(value)
        else:
            return value

    def _deserialize(self, value, attr, data):
        """
        Deserialize value as a uuid.

        Handle both string and UUID object.
        """
        if value is None:
            return None

        try:
            if self.use_uuidformat:
                return str(value)
            else:
                return str(UUID(value))

        except ValueError:
            raise ValidationError('Not a valid UUID.')
