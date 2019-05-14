"""
Matchers for message publishing.

"""
from typing import List

from hamcrest.core.helpers.wrap_matcher import wrap_matcher
from hamcrest.library.collection.issequence_containinginanyorder import (
    IsSequenceContainingInAnyOrder,
)
from hamcrest.library.collection.issequence_containinginorder import IsSequenceContainingInOrder

from microcosm_pubsub.matchers.message import PublishedMessage


def published_messages_for(sns_producer) -> List[PublishedMessage]:
    return list(PublishedMessage.iter_from_sns_producer(sns_producer))


class NonStrictPublishingMatcher(IsSequenceContainingInAnyOrder):
    """
    Matcher that validates a sequence of published messages extracted from an SNSProducer.

    """
    def matches(self, sns_producer, mismatch_description=None):
        return super().matches(
            published_messages_for(sns_producer),
            mismatch_description,
        )


class StrictPublishingMatcher(IsSequenceContainingInOrder):
    """
    Matcher that validates a sequence of published messages extracted from an SNSProducer.

    """
    def matches(self, sns_producer, mismatch_description=None):
        return super().matches(
            published_messages_for(sns_producer),
            mismatch_description,
        )


def published(*matchers, strict=True):
    return StrictPublishingMatcher([
        wrap_matcher(matcher)
        for matcher in matchers
    ])


def published_inanyorder(*matchers, strict=True):
    return NonStrictPublishingMatcher([
        wrap_matcher(matcher)
        for matcher in matchers
    ])


def published_nothing(*matchers):
    return StrictPublishingMatcher([])
