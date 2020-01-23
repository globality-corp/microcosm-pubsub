from dataclasses import dataclass
from time import time
from typing import Dict, Optional

from microcosm.api import binding

from microcosm_pubsub.constants import PUBLISHED_KEY


@dataclass
class PublishedMessage:
    """
    Container class encapsulating all of the necessary components for
    publishing a given message

    """
    media_type: str
    message: str
    opaque_data: dict

    # Deprecated: those attributes are SNS-specific and will be removed from this class
    message_attributes: Optional[Dict[str, Dict[str, str]]] = None
    topic_arn: Optional[str] = None


@binding("published_message_builder")
class PublishedMessageBuilder:
    def __init__(self, graph):
        self.opaque = graph.opaque
        self.pubsub_message_schema_registry = graph.pubsub_message_schema_registry

    def __call__(self, media_type, uri=None, opaque_data=None, **kwargs) -> PublishedMessage:
        """
        Build a message for publishing

        """
        if opaque_data is None:
            opaque_data = dict()

        if self.opaque is not None:
            opaque_data.update(self.opaque.as_dict())

        opaque_data[PUBLISHED_KEY] = str(time())

        message = self.pubsub_message_schema_registry.find(media_type).encode(
            opaque_data=opaque_data,
            media_type=media_type,
            uri=uri,
            **kwargs
        )
        return PublishedMessage(
            media_type=media_type,
            message=message,
            opaque_data=opaque_data,
        )
