"""
Test backoff policies.

"""
from unittest.mock import patch

from hamcrest import assert_that, equal_to, is_

from microcosm_pubsub.backoff import ExponentialBackoffPolicy, NaiveBackoffPolicy
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

    with patch.object(backoff_policy, "randint") as mocked:
        mocked.return_value = 2
        assert_that(
            backoff_policy.compute_backoff_timeout(message, None),
            is_(equal_to(2)),
        )

        mocked.assert_called_with(1, 3)

def test_scaled_exponential_timeout_with_configurable_backoff():
    message = SQSMessage(
        None, None, None, None, None,
        approximate_receive_count=2.5,
    )
    backoff_policy = ExponentialBackoffPolicy(backoff_factor=3)

    with patch.object(backoff_policy, "randint") as mocked:
        mocked.return_value = 14
        assert_that(
            backoff_policy.compute_backoff_timeout(message, None),
            is_(equal_to(14)),
        )

        mocked.assert_called_with(1, 14)

