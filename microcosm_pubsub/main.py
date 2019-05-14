"""
PubSub CLI

"""
from __future__ import print_function

from argparse import ArgumentParser
from json import dumps
from sys import stdout

from microcosm.api import create_object_graph
from microcosm.loaders import load_from_dict

from microcosm_pubsub.conventions import changed, created


def main():
    args = parse_args()
    args.func(args)


def parse_args():
    parser = ArgumentParser()

    # common arguments
    parser.add_argument(
        "--debug",
        action="store_true",
    )
    parser.add_argument(
        "--profile",
    )
    parser.add_argument(
        "--region",
    )

    subparsers = parser.add_subparsers()
    write_parser = subparsers.add_parser("write")
    write_parser.add_argument(
        "--topic-arn",
        required=True,
    )
    write_parser.add_argument(
        "--uri",
        required=True,
    )
    write_parser.add_argument(
        "--media-type",
        required=True,
    )
    write_parser.set_defaults(func=write)

    read_parser = subparsers.add_parser("read")
    read_parser.add_argument(
        "--queue-url",
        required=True,
    )
    read_parser.add_argument(
        "--nack",
        action="store_true",
    )
    read_parser.add_argument(
        "--nack-timeout",
        default=1,
        type=int,
    )
    read_parser.set_defaults(func=read)
    return parser.parse_args()


def write(args):
    loader = load_from_dict(
        sns_producer=dict(
            profile_name=args.profile,
            region_name=args.region,
        ),
        sns_topic_arns=dict(
            default=args.topic_arn,
        ),
    )

    graph = create_object_graph("pubsub", debug=args.debug, loader=loader)
    graph.use("pubsub_message_schema_registry")
    graph.use("sns_topic_arns")
    graph.use("sns_producer")
    graph.lock()

    if args.media_type.startswith("application"):
        media_type = args.media_type
    else:
        media_type = created(args.media_type)

    message_id = graph.sns_producer.produce(
        media_type=media_type,
        uri=args.uri,
    )
    print("Wrote SNS message: {}".format(message_id))  # noqa


def read(args):
    loader = load_from_dict(
        sqs_consumer=dict(
            profile_name=args.profile,
            region_name=args.region,
            sqs_queue_url=args.queue_url,
        ),
    )
    graph = create_object_graph("pubsub", debug=args.debug, loader=loader)
    graph.use("sqs_consumer")
    graph.lock()

    for message in graph.sqs_consumer.consume():
        print("Read SQS message: {} ({})".format(  # noqa
            message.message_id,
            message.approximate_receive_count,
        ))
        if args.nack:
            message.nack(args.nack_timeout)
        else:
            message.ack()


def make_naive_message():
    parser = ArgumentParser()

    parser.add_argument(
        "--action",
        choices=["created", "changed"],
    )
    parser.add_argument(
        "--resource-name",
    )
    parser.add_argument(
        "--uri",
    )
    args = parser.parse_args()

    action = dict(created=created, changed=changed)[args.action]

    stdout.write(dumps(dict(mediaType=action(args.resource_name), uri=args.uri)))
