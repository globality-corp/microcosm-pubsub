"""
Test backoff policies.

"""
from hamcrest import assert_that, equal_to, is_

from microcosm_pubsub.backoff import (
    ExponentialBackoffPolicy,
    ExponentialBackoffBaseJitterPolicy,
    NaiveBackoffPolicy,
    MAX_BACKOFF_TIMEOUT,
)
from microcosm_pubsub.message import SQSMessage


def test_default_timeout():
    message = SQSMessage(
        None, None, None, None, None,
        approximate_receive_count=1,
    )
    backoff_policy = NaiveBackoffPolicy(42)

    assert_that(
        backoff_policy.compute_backoff_timeout(message, None),
        is_(equal_to(42)),
    )


def test_message_timeout():
    message = SQSMessage(
        None, None, None, None, None,
        approximate_receive_count=1,
    )
    backoff_policy = NaiveBackoffPolicy(42)

    assert_that(
        backoff_policy.compute_backoff_timeout(message, 77),
        is_(equal_to(77)),
    )


def test_exponential_timeout():
    message = SQSMessage(
        None, None, None, None, None,
        approximate_receive_count=1,
    )
    backoff_policy = ExponentialBackoffPolicy()

    assert_that(
        backoff_policy.compute_backoff_timeout(message, None),
        is_(equal_to(1)),
    )


def test_scaled_exponential_timeout():
    message = SQSMessage(
        None, None, None, None, None,
        approximate_receive_count=2,
    )
    backoff_policy = ExponentialBackoffPolicy()

    time = backoff_policy.compute_backoff_timeout(message, None)

    assert time <= MAX_BACKOFF_TIMEOUT
    assert 1 <= time
    assert time <= 1 + 2**2


def test_scaled_exponential_base_jitter_timeout():
    for i in range(0, 30):
        message = SQSMessage(
            None, None, None, None, None,
            approximate_receive_count=i,
        )
        backoff_policy = ExponentialBackoffBaseJitterPolicy()

        time = backoff_policy.compute_backoff_timeout(message, None)

        # once we reach max -> we should always return max
        assert time <= MAX_BACKOFF_TIMEOUT
        assert i <= time
        assert time <= i + 2**i/2
