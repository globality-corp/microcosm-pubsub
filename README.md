# microcosm_pubsub

PubSub with SNS/SQS

[![Circle CI](https://circleci.com/gh/globality-corp/microcosm-pubsub/tree/develop.svg?style=svg)](https://circleci.com/gh/globality-corp/microcosm-pubsub/tree/develop)


## Conventions

 -  AWS credentials are loaded out-of-band, either using the usual environment variables or dotfile
    or via instance profiles. In other words: credentials are NOT explicitly configured here.

 -  Messages have a `media_type`; most message processing decisions key on this value.

 -  Messages are published to a `sns_topic_arn` based on their `media_type`; there may be multiple topics
    used by a single message producer, but each message is only published to a single topic.

 -  Messages are consumed from a single `sqs_queue_url`; there may be multiple queues, but each is managed
    by separate consumers.


## CLI

For testing purposes, the producer and consumer functions can be invoked from the CLI.

To produce messages:

    sns-produce --topic-arn <topic-arn>

To consume messages:

    sqs-consume --queue-url <queue-url>


## Producing Messages

The producer takes a media type and message content and returns a message id:

    message_id = graph.sns_producer.produce(media_type, message_content)

Message content may be passed as a dictionary, as keyword args, or both:

    message_id = graph.sns_producer.produce(media_type, dict(foo="bar"), bar="baz")


## Consuming Messages

The consumer returns a list of (possibly zero) messages:

    messages = graph.sqs_consumer.consume()

Messages should be explicitly acknowledged after processing:

    for message in messages:
       process(message.content)
       message.ack()

Messages act as context managers; in this mode, messsages will automatically acknowledge themselves if
no exception is raised during processing:

    for message in messages:
        with message:
            process(message.content)


## Validation

Messages use [marshmallow](http://marshmallow.readthedocs.org/en/latest/index.html) schemas for validation.

Most schemas should extend the `microcosm_pubsub.codecs.PubSubMessageSchema` base and implement its
`deserialize_media_type` function:

    class ExampleSchema(PubSubMessageSchema):
        message = fields.String(required=True)

        def deserialize_media_type(self, obj):
            return "application/vnd.globality.pubsub.example"
