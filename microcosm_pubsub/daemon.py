"""
Consume Daemon main.

"""
import microcosm.opaque  # noqa
from microcosm_daemon.api import SleepNow
from microcosm_daemon.daemon import Daemon


class ConsumerDaemon(Daemon):

    def make_arg_parser(self):
        parser = super(ConsumerDaemon, self).make_arg_parser()
        parser.add_argument("--sqs-queue-url")
        return parser

    def schema_mappings(self):
        """
        Define the PubSub message media-type to schema mappings.

        """
        return dict()

    def create_object_graph(self, args):
        graph = super(ConsumerDaemon, self).create_object_graph(args)
        for media_type, schema_cls in self.schema_mappings.items():
            self.graph.pubsub_message_schema_registry.register(media_type, schema_cls)
        return graph

    @property
    def defaults(self):
        if not self.args.sqs_queue_url:
            return dict()

        return dict(
            sqs_consumer=dict(
                sqs_queue_url=self.args.sqs_queue_url,
            ),
        )

    @property
    def components(self):
        return super(ConsumerDaemon, self).components + [
            "opaque",
            "pubsub_message_schema_registry",
            "sqs_message_handler_registry",
            "sqs_consumer",
            "sqs_message_dispatcher",
        ]

    def __call__(self, graph):
        """
        Implement daemon by sinking messages from the consumer to a dispatcher function.

        """
        result = graph.sqs_message_dispatcher.handle_batch()
        if not result.message_count:
            raise SleepNow()
