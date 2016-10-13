#!/usr/bin/env python
from setuptools import find_packages, setup

project = "microcosm_pubsub"
version = "0.19.0"

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
    install_requires=[
        "boto3>=1.3.0",
        "marshmallow>=2.6.1",
        "microcosm>=0.13.0",
        "microcosm-daemon>=0.8.0",
        "microcosm-logging>=0.12.0",
    ],
    setup_requires=[
        "nose>=1.3.6",
    ],
    dependency_links=[
    ],
    entry_points={
        "microcosm.factories": [
            "sqs_message_context = microcosm_pubsub.context:configure_sqs_message_context",
            "pubsub_message_schema_registry = microcosm_pubsub.registry:configure_schema_registry",
            "sqs_consumer = microcosm_pubsub.consumer:configure_sqs_consumer",
            "sqs_envelope = microcosm_pubsub.envelope:configure_sqs_envelope",
            "sqs_message_dispatcher = microcosm_pubsub.dispatcher:configure",
            "sqs_message_handler_registry = microcosm_pubsub.registry:configure_handler_registry",
            "sns_producer = microcosm_pubsub.producer:configure_sns_producer",
            "sns_topic_arns = microcosm_pubsub.producer:configure_sns_topic_arns",
        ]
    },
    tests_require=[
        "coverage>=3.7.1",
        "mock>=1.0.1",
        "PyHamcrest>=1.8.5",
    ],
)
