"""
PublishBatchMessage handler.

"""

from microcosm.api import binding
from microcosm_logging.decorators import logger

from microcosm_pubsub.conventions import created
from microcosm_pubsub.decorators import handles
from microcosm_pubsub.producer import PubsubMessage, SNSProducer


@binding("publish_message_batch")
@handles(created("BatchMessage"))
@logger
class PublishBatchMessage:

    def __init__(self, graph):
        self.sns_producer: SNSProducer = graph.sns_producer

    def __call__(self, message):
        messages = message["messages"]
        for message in messages:
            pubsub_message = PubsubMessage(
                media_type=message["media_type"],
                message=message["message"],
                message_attributes=message["message_attributes"],
                opaque_data=message["opaque_data"],
                topic_arn=message["topic_arn"],
            )

            self.sns_producer.publish_message(pubsub_message)
