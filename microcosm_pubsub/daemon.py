"""
Consume Daemon main.

"""
from microcosm.loaders import load_each, load_from_dict
from microcosm_daemon.api import SleepNow
from microcosm_daemon.daemon import Daemon

from microcosm_pubsub.consumer import STDIN
from microcosm_pubsub.envelope import NaiveSQSEnvelope, SQSEnvelope


class ConsumerDaemon(Daemon):

    def __init__(self):
        super().__init__()
        self.bound_handlers = None

    def make_arg_parser(self):
        parser = super().make_arg_parser()
        parser.add_argument("--stdin", action="store_true")
        parser.add_argument("--sqs-queue-url")
        parser.add_argument("--envelope", choices=[
            envelope_cls.__name__
            for envelope_cls in SQSEnvelope.__subclasses__()
        ])
        return parser

    def create_object_graph_components(self, graph):
        super().create_object_graph_components(graph)
        self.bound_handlers = graph.sqs_message_handler_registry.compute_bound_handlers(
            self.components,
        )

    def run_state_machine(self):
        for media_type, handler in self.bound_handlers.items():
            self.graph.logger.info("Handling: {} with handler: {}".format(
                media_type,
                handler.__name__,
            ))

        super().run_state_machine()

    @property
    def defaults(self):
        config = dict()

        if self.args.stdin:
            config.update(
                sqs_envelope=dict(
                    strategy_name=NaiveSQSEnvelope.__name__,
                ),
                sqs_consumer=dict(
                    sqs_queue_url=STDIN,
                ),
            )

        if self.args.sqs_queue_url:
            config.update(
                sqs_consumer=dict(
                    sqs_queue_url=self.args.sqs_queue_url,
                ),
            )

        if self.args.envelope:
            config.update(
                sqs_envelope=dict(
                    strategy_name=self.args.envelope,
                ),
            )

        return config

    @property
    def components(self):
        return super().components + [
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
        results = graph.sqs_message_dispatcher.handle_batch(self.bound_handlers)
        if not results:
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

        return super().create_for_testing(
            sqs_queue_url="queue",
            loader=loader,
            envelope=None,
            stdin=False,
            **kwargs
        )
