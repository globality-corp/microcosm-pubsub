"""
Message producer.

"""
from collections import defaultdict

from boto3 import client
from microcosm.api import defaults

from microcosm_pubsub.errors import TopicNotDefinedError


class SNSProducer(object):
    """
    Produces messages to SNS topics.

    """
    def __init__(self, sns_client, sns_topic_arns, pubsub_message_codecs):
        self.sns_client = sns_client
        self.sns_topic_arns = sns_topic_arns
        self.pubsub_message_codecs = pubsub_message_codecs

    def produce(self, media_type, dct=None, **kwargs):
        """
        Produce a message.

        :returns: the message id

        """
        topic_arn = self.choose_topic_arn(media_type)
        content = self.pubsub_message_codecs[media_type].encode(dct, **kwargs)
        result = self.sns_client.publish(
            TopicArn=topic_arn,
            Message=content,
        )
        return result["MessageId"]

    def choose_topic_arn(self, media_type):
        """
        Choose a topic for this type of message.

        """
        try:
            topic_arn = self.sns_topic_arns[media_type]
        except KeyError:
            topic_arn = None

        if topic_arn is None:
            raise TopicNotDefinedError("No topic arn was registered for messages of type: {}".format(
                media_type,
            ))
        return topic_arn


@defaults(
    default=None,
    mappings={},
)
def configure_sns_topic_arns(graph):
    """
    Configure a mapping from message types to topic ARNs.

    """
    if graph.config.sns_topic_arns.default is None:
        sns_topic_arns = dict()
    else:
        sns_topic_arns = defaultdict(lambda: graph.config.sns_topic_arns.default)

    sns_topic_arns.update(graph.config.sns_topic_arns.mappings)

    return sns_topic_arns


def configure_sns_producer(graph):
    """
    Configure an SNS producer.

    """
    if graph.metadata.testing:
        from mock import MagicMock
        sns_client = MagicMock()
    else:
        sns_client = client("sns")

    return SNSProducer(
        sns_client=sns_client,
        sns_topic_arns=graph.sns_topic_arns,
        pubsub_message_codecs=graph.pubsub_message_codecs,
    )
