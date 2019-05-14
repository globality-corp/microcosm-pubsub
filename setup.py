#!/usr/bin/env python
from setuptools import find_packages, setup


project = "microcosm-pubsub"
version = "1.24.0"

setup(
    name=project,
    version=version,
    description="PubSub with SNS/SQS",
    author="Globality Engineering",
    author_email="engineering@globality.com",
    url="https://github.com/globality-corp/microcosm-pubsub",
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    include_package_data=True,
    zip_safe=False,
    python_requires=">=3.6",
    install_requires=[
        "boto3>=1.5.8",
        "dataclasses;python_version<'3.7'",
        "marshmallow>=2.15.0",
        "microcosm>=2.0.0",
        "microcosm-daemon>=1.0.0",
        "microcosm-logging>=1.3.0",
    ],
    extras_require={
        "metrics": "microcosm-metrics>=2.2.0",
    },
    setup_requires=[
        "nose>=1.3.6",
    ],
    dependency_links=[
    ],
    entry_points={
        "console_scripts": [
            "publish-naive = microcosm_pubsub.main:make_naive_message",
            "pubsub = microcosm_pubsub.main:main",
        ],
        "microcosm.factories": [
            "pubsub_message_schema_registry = microcosm_pubsub.registry:configure_schema_registry",
            "pubsub_lifecycle_change = microcosm_pubsub.conventions:LifecycleChange",
            "pubsub_send_batch_metrics = microcosm_pubsub.metrics:PubSubSendBatchMetrics",
            "pubsub_send_metrics = microcosm_pubsub.metrics:PubSubSendMetrics",
            "sqs_message_context = microcosm_pubsub.context:SQSMessageContext",
            "sqs_consumer = microcosm_pubsub.consumer:configure_sqs_consumer",
            "sqs_envelope = microcosm_pubsub.envelope:configure_sqs_envelope",
            "sqs_message_dispatcher = microcosm_pubsub.dispatcher:SQSMessageDispatcher",
            "sqs_message_handler_registry = microcosm_pubsub.registry:configure_handler_registry",
            "sns_producer = microcosm_pubsub.producer:configure_sns_producer",
            "sns_topic_arns = microcosm_pubsub.producer:configure_sns_topic_arns",
        ]
    },
    tests_require=[
        "coverage>=3.7.1",
        "PyHamcrest>=1.8.5",
    ],
)
