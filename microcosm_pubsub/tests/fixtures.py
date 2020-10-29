"""
Test fixtures.

"""
from argparse import Namespace

from marshmallow import Schema, fields
from microcosm.api import binding
from microcosm.caching import ProcessCache
from microcosm.loaders import load_each, load_from_dict

from microcosm_pubsub.codecs import PubSubMessageSchema
from microcosm_pubsub.conventions import created, deleted
from microcosm_pubsub.daemon import ConsumerDaemon
from microcosm_pubsub.decorators import handles, schema
from microcosm_pubsub.errors import SkipMessage


@schema
class DerivedSchema(PubSubMessageSchema):
    """
    A schema that is derived from `PubSubMessageSchema`

    """
    MEDIA_TYPE = "application/vnd.microcosm.derived"

    data = fields.String(required=True)


@schema
class DuckTypeSchema(Schema):
    """
    A duck typed schema

    """
    MEDIA_TYPE = "application/vnd.microcosm.duck"

    quack = fields.String()


@handles(DuckTypeSchema)
@handles(created("Foo"))
@handles(deleted("Foo"))
@handles(DerivedSchema.MEDIA_TYPE)
def noop_handler(message):
    return True


@handles(created("IgnoredResource"))
def skipping_handler(message):
    raise SkipMessage("Failed")


class ExampleDaemon(ConsumerDaemon):

    @property
    def name(self):
        return "example"

    @property
    def components(self):
        return super().components + [
            "noop_handler",
        ]


@binding("noop_handler")
def configure_noop_handler(graph):
    return noop_handler


class SQSReaderExampleDaemon(ConsumerDaemon):
    """
    For testing SQSJsonReader(and other real readers)
    Object needs to be initialized with `event` parameter
    """
    @property
    def name(self):
        return "example"

    @property
    def components(self):
        return super().components + [
            "noop_handler",
        ]

    @classmethod
    def create_for_testing(cls, loader=None, cache=None, **kwargs):
        mock_config = load_from_dict(
            sns_producer=dict(
                mock_sns=False,
            ),
        )

        if loader is None:
            loader = mock_config
        else:
            loader = load_each(loader, mock_config)

        if cache is None:
            scope = cls.__name__
            cache = ProcessCache(scope=scope)
        # To test SQS readers we pass event here
        daemon = cls(enable_lambda_mode=kwargs.get("enable_lambda_mode", False))
        daemon.args = Namespace(
            debug=False,
            testing=True,
            sqs_queue_url="queue",
            loader=loader,
            envelope=None,
            stdin=False,
            **kwargs
        )
        daemon.graph = daemon.create_object_graph(daemon.args, cache=cache, loader=loader)
        return daemon
