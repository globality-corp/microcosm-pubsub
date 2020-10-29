"""
Consume Daemon main.

"""
import argparse

from microcosm.loaders import load_each, load_from_dict
from microcosm_daemon.api import SleepNow
from microcosm_daemon.daemon import Daemon

from microcosm_pubsub.consumer import STDIN
from microcosm_pubsub.envelope import LambdaSQSEnvelope, NaiveSQSEnvelope, SQSEnvelope


class ConsumerDaemon(Daemon):

    def __init__(self, enable_lambda_mode=False):
        super().__init__()
        self.bound_handlers = None
        self.enable_lambda_mode = enable_lambda_mode

    def make_arg_parser(self):
        parser = super().make_arg_parser()

        parser.add_argument("--stdin", action="store_true")
        parser.add_argument("--sqs-queue-url")
        parser.add_argument("--envelope", choices=[
            envelope_cls.__name__
            for envelope_cls in SQSEnvelope.__subclasses__()
        ])

        if self.enable_lambda_mode:
            # during lambda execution, we do not need to parse arguments, so adding a below catch all args list
            parser.add_argument('args', nargs=argparse.REMAINDER)

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
        self.graph.logger.info("Local starting daemon {}".format(self.name))
        with self.graph.error_policy:
            self.graph.sqs_message_dispatcher.handle_batch(self.bound_handlers)

    @classmethod
    def make_lambda_handler(cls):
        # initializing graph object during first call, and any subsequent calls would reuse execution context
        # unless the execution context is unavailable
        daemon = cls(enable_lambda_mode=True)
        daemon.initialize()

        def handler(event, context):
            """
            AWS Lambda function handler.
            """
            # warmp events generated via Cloudwatch to keep Lambda's warm, which means that past invocation's
            # execution context would be used again. If Lambda's become cold, that container startup time and
            # microcosm graph loading time would slow the message processing
            if "warm" in event:
                daemon.graph.logger.debug("Processing warm-up cloudwatch event")
                return "warming up"

            # we configure SQS Queue to use batches of 1,
            # so received event contains
            # another stringified json with actual message inside
            daemon.graph.logger.debug("Processing SQS event: {}".format(event["Records"][0]))
            daemon.graph.sqs_consumer.set_sqs_event(event["Records"][0])
            daemon.process()
            return "event processed"

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
                    enable_lambda_mode=self.enable_lambda_mode
                ),
            )

        if self.args.sqs_queue_url:
            config.update(
                sqs_consumer=dict(
                    sqs_queue_url=self.args.sqs_queue_url,
                    enable_lambda_mode=self.enable_lambda_mode
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
        if self.enable_lambda_mode:
            config.update(
                sqs_envelope=dict(
                    strategy_name=LambdaSQSEnvelope.__name__,
                ),
                sqs_consumer=dict(
                    enable_lambda_mode=self.enable_lambda_mode,
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
