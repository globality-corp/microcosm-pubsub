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
messages and dispatch them to user-defined worker functions. Usage involves declaring a schema,
declaring a handler function, and declaring a deamon that runs them.


Import the baseclass, define a schema, and decorate it with `@schema`:

    from marshmallow import fields

    from microcosm.api import binding, create_object_graph

    from microcosm_pubsub.daemon import ConsumerDaemon
    from microcosm_pubsub.decorators import handles, schema


    @schema
    class SimpleSchema(PubSubMessageSchema):
        """
        A single schema that just sends a text string.

        """
        MEDIA_TYPE = "application/vnd.globality.pubsub.simple"

        message = fields.String(required=True)
        timestamp = fields.Float(required=True)

        def deserialize_media_type(self, obj):
            return SimpleSchema.MEDIA_TYPE

Define a function that handles messages for the schema and decorate it with `@handles` to
indicate that it handles your schema type. While plain functions, suffice, most real-world
handlers will be a class with its own `@binding` to pass other collaborators:

    @binding("simple_handler")
    @handles(SimpleSchema)
    class SimpleHandler:
        def __init__(self, graph):
            self.collaborator = graph.collaborator

        def __call__(self, message):
            self.collaborator.do_something(message)
            return True


Subclass the `ConsumerDaemon` and override any required attributes (notably `name`):

    class SimpleConsumerDaemon(ConsumerDaemon):

        @property
        def name(self):
            return "example"


Declare a main function for the daemon either using `setuptools` entry points (preferred) or
the usual boilerplate:

    if __name__ == '__main__':
        daemon = SimpleConsumerDaemon()
        daemon.run()

When running the daemon, pass the `--sqs-queue-url` arguments and the usual `--testing`/`--debug` flags as appropriate:

    python /path/to/simple_daemon.py --sqs-queue-url <queue name> --debug


## Configuring Sentry Support

An example of a full config to enable and customise the usage of sentry in an
Application using `microcosm_pubsub`.

```json
{
  "sentry_logging": {
    "enabled": true,
    "dsn": "https://some-value@numbers.ingest.sentry.io/project-id",
    "custom_tags_mapping": {
      "opaque-key": "sentry-tag-name"
    },
    "custom_user_id": "my-opaque-user-id"
  }
}
```

- `sentry_logging_pubsub.enabled` - boolean
- `sentry_logging_pubsub.dsn` - url - the url of the project that errors will be
 sent to
- `sentry_logging_pubsub.custom_tags_mapping` - key/value mapping - defines what data
 stored in `Opaque` data should be sent to sentry and what the tag in sentry
 should be.
- `sentry_logging_pubsub.custom_user_id` - string - defines what data within `Opaque`
should be sent to `sentry` as `user.id`.

### Overriding the default before_send
`before_send` is a function that runs before each event that is sent to sentry.
It is used to remove sensitive data, the default one provided is reasonably
strict removing most data in a quite a generic way. To customise this behaviour
in an Application the following can be done.

```python
from microcosm.decorators import binding
from microcosm.api import create_object_graph


@binding("sentry_before_send")
def custom_before_send_factory(graph):
    def before_send(event, hint):
        # this would disable all filtering
        return event
    return before_send

graph = create_object_graph("example")
graph.use("sentry_before_send")
```
