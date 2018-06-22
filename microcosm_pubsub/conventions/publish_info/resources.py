"""
Publish Info resources.

"""
from marshmallow import fields, Schema


class PublishInfoSchema(Schema):
    mediaType = fields.String(attribute="media_type")
    route = fields.String()
    callModule = fields.String(attribute="call_module")
    callFunction = fields.String(attribute="call_function")
    count = fields.Integer()
