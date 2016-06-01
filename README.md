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


## Validation

Messages use [marshmallow](http://marshmallow.readthedocs.org/en/latest/index.html) schemas for validation.

Most schemas should extend the `microcosm_pubsub.codecs.PubSubMessageSchema` base and implement its
`deserialize_media_type` function:

    class ExampleSchema(PubSubMessageSchema):
        message = fields.String(required=True)

        def deserialize_media_type(self, obj):
            return "application/vnd.globality.pubsub.example"


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


## Asynchronous Workers

The `ConsumerDaemon` base class supports creating asynchronous workers ("daemons") that consume
messages and dispatch them to user-defined worker functions. Usage requires creating a subclass
that defines the `name`, `schema_mappings`, and `handler_mappings` properties. Here's how:


Import the baseclass and one or more schemas:

    from microcosm_pubsub.daemon import ConsumerDaemon
    from microcosm_pubsub.main import SimpleSchema


Define a function that handles messages for each kind of schema:

    def handle_simple(message):
        print message
        return True


Subclass the `ConsumerDaemon` to provide mappings from each schema's media type to
its schema class and handler function:

    class SimpleConsumerDaemon(ConsumerDaemon):

        @property
        def name(self):
            return "example"

        @property
        def handler_mappings(self):
            return {
                SimpleSchema.MEDIA_TYPE: handle_simple,
            }

        @property
        def schema_mappings(self):
            return {
                SimpleSchema.MEDIA_TYPE: SimpleSchema,
            }


Declare a main function for the daemon either using `setuptools` entry points (preferred) or
the usual boilerplate:

    if __name__ == '__main__':
        daemon = SimpleConsumerDaemon()
        daemon.run()

When running the daemon, pass the `--sqs-queue-url` arguments and the usual `--testing`/`--debug` flags as appropriate:

    python /path/to/simple_daemon.py --sqs-queue-url <queue name> --debug
