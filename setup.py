#!/usr/bin/env python
from setuptools import find_packages, setup

project = "microcosm_pubsub"
version = "0.10.2"

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
        "microcosm>=0.7.0",
        "microcosm-daemon>=0.2.0",
    ],
    setup_requires=[
        "nose>=1.3.6",
    ],
    dependency_links=[
    ],
    entry_points={
        "console_scripts": [
            "sns-produce = microcosm_pubsub.main:produce",
            "sqs-consume = microcosm_pubsub.main:consume",
            "simple-daemon = microcosm_pubsub.main:main",
        ],
        "microcosm.factories": [
            "sqs_message_context = microcosm_pubsub.context:configure_sqs_message_context",
            "pubsub_message_codecs = microcosm_pubsub.codecs:configure_pubsub_message_codecs",
            "sqs_consumer = microcosm_pubsub.consumer:configure_sqs_consumer",
            "sqs_envelope = microcosm_pubsub.envelope:configure_sqs_envelope",
            "sqs_message_dispatcher = microcosm_pubsub.dispatcher:configure_sqs_message_dispatcher",
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
