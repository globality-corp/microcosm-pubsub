import json
from unittest.mock import ANY, patch

from hamcrest import assert_that, equal_to
from microcosm.api import create_object_graph

from microcosm_pubsub import tracing
from microcosm_pubsub.message import SQSMessage
from microcosm_pubsub.result import MessageHandlingResultType
from microcosm_pubsub.tests.fixtures import DerivedSchema, ExampleDaemon


def test_trace_message_publish():
    def loader(metadata):
        return dict(
            sns_topic_arns=dict(
                default="topic-name",
            )
        )

    graph = create_object_graph("example", testing=True, loader=loader)
    graph.sns_producer.sns_client.publish.return_value = dict(MessageId="message-id-1234")

    with patch.object(tracing, "oneagent") as oneagent, \
            patch.object(tracing, "ChannelType"), \
            patch.object(tracing, "Channel") as channel, \
            patch.object(tracing, "MessagingDestinationType") as messagingdestinationtype:
        sdk = oneagent.get_sdk.return_value
        tracer_manager = sdk.trace_outgoing_message.return_value
        tracer_obj = tracer_manager.__enter__.return_value
        tracer_obj.outgoing_dynatrace_string_tag = "tag-1234"

        message_id = graph.sns_producer.produce(
            DerivedSchema.MEDIA_TYPE,
            data="data",
            opaque_data={"X-Request-Id": "req-id-1234"}
        )
        assert_that(message_id, equal_to("message-id-1234"))

        graph.sns_producer.sns_client.publish.assert_called_once_with(
            Message=ANY,
            MessageAttributes=dict(
                media_type=dict(
                    DataType='String',
                    StringValue='application/vnd.microcosm.derived'
                ),
            ),
            TopicArn='topic-name'
        )
        message = json.loads(graph.sns_producer.sns_client.publish.call_args[1]['Message'])
        assert_that(message["opaqueData"]["X-Request-Id"], equal_to("req-id-1234"))
        assert_that(message["opaqueData"]["x-dynatrace"], equal_to("tag-1234"))

        sdk.create_messaging_system_info.assert_called_once_with(
            "SNS",
            "topic-name",
            messagingdestinationtype.TOPIC,
            channel.return_value
        )
        sdk.trace_outgoing_message.assert_called_once_with(
            sdk.create_messaging_system_info.return_value
        )

        tracer_manager.__enter__.assert_called_once_with()
        tracer_obj.set_vendor_message_id.assert_called_once_with(
            "message-id-1234"
        )
        tracer_obj.set_correlation_id.assert_called_once_with(
            "req-id-1234"
        )
        tracer_manager.__exit__.assert_called_once_with(None, None, None)


def test_trace_message_dispatch():
    daemon = ExampleDaemon.create_for_testing()
    graph = daemon.graph
    dispatcher = graph.sqs_message_dispatcher
    content = dict(
        bar="baz",
        uri="http://example.com",
        opaque_data={
            "X-Request-Id": "req-id-5678",
            "x-dynatrace": "tag-5678",
        }
    )
    message = SQSMessage(
        approximate_receive_count=0,
        consumer=graph.sqs_consumer,
        content=content,
        media_type=DerivedSchema.MEDIA_TYPE,
        message_id="message-id-5678",
        receipt_handle=None,
    )
    graph.sqs_consumer.sqs_client.reset_mock()

    with patch.object(tracing, "oneagent") as oneagent, \
            patch.object(tracing, "ChannelType"), \
            patch.object(tracing, "Channel") as channel, \
            patch.object(tracing, "MessagingDestinationType") as messagingdestinationtype:

        result = dispatcher.handle_message(
            message=message,
            bound_handlers=daemon.bound_handlers,
        )
        assert_that(result.result, equal_to(MessageHandlingResultType.SUCCEEDED))

        sdk = oneagent.get_sdk.return_value

        sdk.create_messaging_system_info.assert_called_once_with(
            "SQS",
            "queue",
            messagingdestinationtype.QUEUE,
            channel.return_value
        )
        sdk.trace_incoming_message_process.assert_called_once_with(
            sdk.create_messaging_system_info.return_value,
            str_tag="tag-5678",
        )
        tracer_manager = sdk.trace_incoming_message_process.return_value
        tracer_manager.__enter__.assert_called_once_with()
        tracer_obj = tracer_manager.__enter__.return_value
        tracer_obj.set_vendor_message_id.assert_called_once_with(
            "message-id-5678"
        )
        tracer_obj.set_correlation_id.assert_called_once_with(
            "req-id-5678"
        )
        tracer_manager.__exit__.assert_called_once_with(None, None, None)
