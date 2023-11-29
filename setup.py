#!/usr/bin/env python
from setuptools import find_packages, setup


project = "microcosm-pubsub"
version = "2.30.0"


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
    python_requires=">=3.9",
    install_requires=[
        "boto3>=1.5.8",
        "marshmallow>=3.0.0",
        "microcosm>=3.5.0",
        "microcosm-caching>=0.2.0",
        "microcosm-daemon>=1.2.0",
        "microcosm-logging>=1.3.0",
        "requests>=2.31.0"
    ],
    extras_require={
        "metrics": "microcosm-metrics>=2.5.0",
        "sentry": "sentry-sdk>=0.14.4",
        "build": [
            "flake8",
            "mypy",
            "PyHamcrest",
            "pytest-cov",
            "pytest",
            "sentry-sdk",
        ]
    },
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
            "pubsub_producer_metrics = microcosm_pubsub.metrics:PubSubProducerMetrics",
            "sqs_message_context = microcosm_pubsub.context:SQSMessageContext",
            "sqs_consumer = microcosm_pubsub.consumer:configure_sqs_consumer",
            "sqs_envelope = microcosm_pubsub.envelope:configure_sqs_envelope",
            "sqs_message_dispatcher = microcosm_pubsub.dispatcher:SQSMessageDispatcher",
            "sqs_message_handler_registry = microcosm_pubsub.registry:configure_handler_registry",
            "sns_producer = microcosm_pubsub.producer:configure_sns_producer",
            "sns_topic_arns = microcosm_pubsub.producer:configure_sns_topic_arns",
            "sentry_logging_pubsub = microcosm_pubsub.sentry:configure_sentry_pubsub",
        ],
    },
)
