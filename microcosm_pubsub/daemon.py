"""
Consume Daemon main.

"""
from abc import abstractproperty

from microcosm_daemon.api import SleepNow
from microcosm_daemon.daemon import Daemon


class ConsumerDaemon(Daemon):

    @abstractproperty
    def sqs_queue_url(self):
        """
        Define the SQS Queue URL.

        """
        pass

    @abstractproperty
    def schema_mappings(self):
        """
        Define the PubSub message media-type to schema mappings.

        """
        pass

    @abstractproperty
    def handler_mappings(self):
        """
        Define the PubSub message media-type to handler mappings.

        """
        pass

    @property
    def defaults(self):
        return {
            "pubsub_message_codecs":  {
                "mappings": self.schema_mappings,
            },
            "sqs_consumer": {
                "sqs_queue_url": self.queue_url,
            },
            "sqs_message_dispatcher": {
                "mappings": self.handler_mappings,
            },
        }

    @property
    def components(self):
        return super(ConsumerDaemon, self).components + [
            "pubsub_message_codecs",
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
