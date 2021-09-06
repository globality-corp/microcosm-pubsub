"""
Support optional tracing using Dynatrace oneagent.

If the dynatrace SDK is not installed, does nothing.

"""
import json
from contextlib import contextmanager

from microcosm.opaque import Opaque

from microcosm_pubsub.message import SQSMessage


try:
    import oneagent
    from oneagent.common import ChannelType, MessagingDestinationType
    from oneagent.sdk import Channel
except ImportError:
    oneagent = None
    ChannelType = None
    MessagingDestinationType = None
    Channel = None


AWS_SNS = "SNS"
AWS_SQS = "SQS"
OPAQUE_TAG_KEY = "x-dynatrace"


class StubOutgoingTracer:
    outgoing_dynatrace_string_tag = None

    def set_vendor_message_id(self, id):
        pass

    def set_correlation_id(self, id):
        pass


def add_trace_to_message(tracer, pubsub_message):
    tag = tracer.outgoing_dynatrace_string_tag
    if tag:
        try:
            message = json.loads(pubsub_message.message)
            message["opaqueData"][OPAQUE_TAG_KEY] = tag
            pubsub_message.message = json.dumps(message)
        except Exception:
            pass


@contextmanager
def trace_outgoing_message(topic_arn: str):
    if not oneagent:
        yield StubOutgoingTracer()
    else:
        sdk = oneagent.get_sdk()
        try:
            topic_name = topic_arn.split(":")[-1]
        except IndexError:
            topic_name = topic_arn

        messaging_system = sdk.create_messaging_system_info(
            AWS_SNS,
            topic_name,
            MessagingDestinationType.TOPIC,
            Channel(ChannelType.TCP_IP, None),
        )
        with sdk.trace_outgoing_message(messaging_system) as tracer:
            yield tracer


@contextmanager
def trace_incoming_message_process(opaque: Opaque, message: SQSMessage, queue_url: str):
    if not oneagent:
        yield
    else:
        try:
            queue_name = queue_url.split("/")[-1]
        except IndexError:
            queue_name = queue_url

        sdk = oneagent.get_sdk()
        messaging_system = sdk.create_messaging_system_info(
            AWS_SQS,
            queue_name,
            MessagingDestinationType.QUEUE,
            Channel(ChannelType.TCP_IP, None),
        )
        tag = opaque.get(OPAQUE_TAG_KEY)
        request_id = opaque.get("X-Request-Id")
        with sdk.trace_incoming_message_process(messaging_system, str_tag=tag) as tracer:
            tracer.set_vendor_message_id(message.message_id)
            if request_id:
                tracer.set_correlation_id(request_id)
            yield
