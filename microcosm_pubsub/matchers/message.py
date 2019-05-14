from dataclasses import dataclass
from json import loads
from typing import Iterable, Mapping, Optional
from unittest.mock import Mock

from hamcrest import not_none
from hamcrest.core.helpers.wrap_matcher import wrap_matcher
from hamcrest.library.object.hasproperty import IsObjectWithProperty


@dataclass
class PublishedMessage:
    """
    A published message.

    """
    topic_arn: str
    message: Mapping[str, str]

    @property
    def media_type(self) -> Optional[str]:
        return self.message.get("mediaType")

    @property
    def uri(self) -> Optional[str]:
        return self.message.get("uri")

    @classmethod
    def from_call(cls, call) -> Optional["PublishedMessage"]:
        """
        Extract a published message from a mock call.

        """
        # NB: _Call is a tuple of (name, args, kwargs)
        name, args, kwargs = call

        # Case 1: we've mocked the boto3 SNS client
        if "TopicArn" in kwargs and "Message" in kwargs:
            topic_arn = kwargs["TopicArn"]
            # NB: we could use the envelope functions to parse the inner message
            message = loads(kwargs["Message"])
            return cls(
                topic_arn=topic_arn,
                message=message,
            )

        # Case 2: we've mocked the entire SNSProducer
        if args and args[0].startswith("application/vnd.globality") and kwargs:
            topic_arn = None

            message = kwargs.copy()
            message.update(mediaType=args[0])

            return cls(
                topic_arn=topic_arn,
                message=message,
            )

        # calls with unexpected format will be filtered
        return None

    @staticmethod
    def iter_from_mock_calls(mock_calls) -> Iterable["PublishedMessage"]:
        """
        Iterate over published messages among mock calls.

        """
        return list(filter(None, (
            PublishedMessage.from_call(mock_call)
            for mock_call in mock_calls
        )))

    @staticmethod
    def iter_from_sns_producer(sns_producer) -> Iterable["PublishedMessage"]:
        """
        Iterate over published messages from a mocked SNSProducer.

        """
        if isinstance(sns_producer, Mock):
            target = sns_producer.produce
        else:
            target = sns_producer.sns_client.publish

        return PublishedMessage.iter_from_mock_calls(target.mock_calls)


class PublishedMessageMatcher(IsObjectWithProperty):
    """
    A matcher that validates a single published message property.

    This matcher is primarily a cosmetric change describe errors in terms of messages
    (and not generic objects).

    """
    def describe_to(self, description):
        return description.append_text(
            f"a message with a {self.property_name} matching ",
        ).append_description_of(
            self.value_matcher,
        )


def has_media_type(match=None):
    # NB: if no matcher is provider, just check that *some* media type exists
    if match is None:
        match = not_none()

    return PublishedMessageMatcher("media_type", wrap_matcher(match))


def has_uri(match=None):
    # NB: if no matcher is provider, just check that *some* uri exists
    if match is None:
        match = not_none()

    return PublishedMessageMatcher("uri", wrap_matcher(match))
