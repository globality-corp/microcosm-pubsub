"""
Command line entry point.

"""
from argparse import ArgumentParser

from marshmallow import fields
from microcosm.api import create_object_graph

from microcosm_pubsub.codecs import PubSubMessageSchema


class SimpleSchema(PubSubMessageSchema):
    """
    A single schema that just sends a text string.

    """
    MEDIA_TYPE = "application/vnd.globality.pubsub.simple"

    message = fields.String(required=True)

    def deserialize_media_type(self, obj):
        return SimpleSchema.MEDIA_TYPE


def produce():
    """
    Produce test messages.

    """
    parser = ArgumentParser()
    parser.add_argument("--count", default=1, type=int)
    parser.add_argument("--message", default="Hello World")
    parser.add_argument("--message-type", default="test")
    parser.add_argument("--topic-arn", required=True)
    args = parser.parse_args()

    def load_config(metadata):
        return dict(
            pubsub_message_codecs=dict(
                default=SimpleSchema,
            ),
            sns_topic_arns=dict(
                default=args.topic_arn,
            ),
        )

    graph = create_object_graph("example", loader=load_config)
    for _ in range(args.count):
        message_id = graph.sns_producer.produce(args.message_type, message=args.message)
        print message_id  # noqa


def consume():
    parser = ArgumentParser()
    parser.add_argument("--queue-url", required=True)
    args = parser.parse_args()

    def load_config(metadata):
        return dict(
            pubsub_message_codecs=dict(
                default=SimpleSchema,
            ),
            sqs_consumer=dict(
                sqs_queue_url=args.queue_url,
            ),
        )

    graph = create_object_graph("example", loader=load_config)
    messages = graph.sqs_consumer.consume()

    for message in messages:
        with message:
            print message.content  # noqa
