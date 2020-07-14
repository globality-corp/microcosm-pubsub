"""
Consume Daemon main.

"""
from microcosm.loaders import load_each, load_from_dict
from microcosm_daemon.api import SleepNow
from microcosm_daemon.daemon import Daemon

from microcosm_pubsub.consumer import STDIN
from microcosm_pubsub.envelope import LambdaSQSEnvelope, NaiveSQSEnvelope, SQSEnvelope


class ConsumerDaemon(Daemon):

    def __init__(self, event=None):
        super().__init__()
        self.bound_handlers = None
        self.sqs_event = event or {}

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

    def process(self):
        """
        Lambda Function method that runs only once
        """
        self.initialize()
        self.graph.logger.info("Local starting daemon {}".format(self.name))
        with self.graph.error_policy:
            self.graph.sqs_message_dispatcher.handle_batch(self.bound_handlers)

    @classmethod
    def make_lambda_handler(cls):
        def handler(event, context):
            """
            AWS Lambda function handler.
            """
            # this is for the warmup event.
            # just return something and don't continue
            if "warm" in event:
                return "warming up"

            # we configure SQS Queue to use batches of 1,
            # so received event contains
            # another stringified json with actual message inside
            daemon = cls(event=event["Records"][0])
            daemon.process()
        return handler

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
                    sqs_event=""
                ),
            )

        if self.args.sqs_queue_url:
            config.update(
                sqs_consumer=dict(
                    sqs_queue_url=self.args.sqs_queue_url,
                    sqs_event=""
                ),
            )

        if self.args.envelope:
            config.update(
                sqs_envelope=dict(
                    strategy_name=self.args.envelope,
                ),
            )
        # for AWS Lambda
        # SQS message comes as function argument
        # e.g
        # ```
        # def handler(event, context):
        #     # processing event
        # ```
        # we don't use command line args here
        if self.sqs_event:
            config.update(
                sqs_envelope=dict(
                    strategy_name=LambdaSQSEnvelope.__name__,
                ),
                sqs_consumer=dict(
                    sqs_event=self.sqs_event,
                    sqs_queue_url="",
                )
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
