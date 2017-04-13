"""
Consume Daemon main.

"""
from microcosm.loaders import load_each, load_from_dict
from microcosm_daemon.api import SleepNow
from microcosm_daemon.daemon import Daemon


class ConsumerDaemon(Daemon):

    def make_arg_parser(self):
        parser = super(ConsumerDaemon, self).make_arg_parser()
        parser.add_argument("--sqs-queue-url")
        return parser

    def run_state_machine(self):
        for media_type in self.graph.sqs_message_handler_registry.keys():
            handler = self.graph.sqs_message_handler_registry[media_type]
            self.graph.logger.info("Handling: {} with handler: {}".format(
                media_type,
                handler.__class__.__name__,
            ))
        super(ConsumerDaemon, self).run_state_machine()

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

    @classmethod
    def create_for_testing(cls, loader=None, **kwargs):
        mock_config = load_from_dict(
            sns_producer=dict(
                mock_sns=False,
            ),
        )

        if loader is None:
            loader = mock_config
        else:
            loader = load_each(loader, mock_config)

        return super(ConsumerDaemon, cls).create_for_testing(
            sqs_queue_url="queue",
            loader=loader,
            **kwargs
        )
