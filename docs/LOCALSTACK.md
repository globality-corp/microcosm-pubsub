# LocalStack

SNS and SQS interactions can be tested with [LocalStack](https://github.com/atlassian/localstack).

Here's how:

 1. Run `LocalStack`

    The easiest way is via Docker:

        docker run -it --rm --name localstack -p 4567-4578:4567-4578 -p 8080:8080 atlassianlabs/localstack

 2. Provision a topic and queue using the [AWS CLI](https://aws.amazon.com/cli/)

        # create a topic
        aws --endpoint-url http://localhost:4575 sns create-topic --name mytopic

        # create a queue
        aws --endpoint-url http://localhost:4576 sqs create-queue --queue-name myqueue

        # subscribe the queue to the topic
        aws --endpoint-url http://localhost:4575 sns subscribe \
            --topic-arn arn:aws:sns:us-east-1:123456789012:mytopic \
            --protocol sqs \
            --notification-endpoint arn:aws:sqs:us-east-1:123456789012:myqueue

    Note that `LocalStack` currently does not validate the `notification-endpoint` correctly; a malformed
    input (such as a queue `url`) will result in errors when publishing to the topic.

 3. Configure the object graph.

    You will need to specify your queue url, you topic ARN, and the `LocalStack` API endpoints. You will
    also need to inform `microcosm-pubsub` that you are using `LocalStack` because the message body it creates
    for subscribed queues does not match what AWS does.

    Example:

        from microcosm.api import create_object_graph
        from microcosm.loaders import load_from_dict

        loader = load_from_dict(
            sqs_consumer=dict(
                endpoint_url="http://localhost:4576",
                sqs_queue_url="http://localhost:4576/123456789012/myqueue",
            ),
            sqs_envelope=dict(
                strategy_name="LocalStackSQSEnvelope",
            ),
            sns_producer=dict(
                endpoint_url="http://localhost:4575",
            ),
            sns_topic_arns=dict(
                default="arn:aws:sns:us-east-1:123456789012:mytopic",
            ),
        )
        graph = create_object_graph("test", loader=loader)
        graph.use(
            "sqs_consumer",
            "sns_producer",
        )
