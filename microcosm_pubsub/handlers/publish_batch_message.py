"""
PublishBatchMessage handler.

"""

from microcosm.api import binding
from microcosm_logging.decorators import logger

from microcosm_pubsub.conventions import created
from microcosm_pubsub.decorators import handles


@binding("publish_message_batch")
@handles(created("BatchMessage"))
@logger
class PublishBatchMessage:

    def __init__(self, graph):
        self.sns_producer = graph.sns_producer

    def __call__(self, message):
        messages = message["messages"]
        for message in messages:
            self.sns_producer.publish_message(
                message["media_type"],
                message["message"],
                message["topic_arn"],
                message["opaque_data"],
            )
