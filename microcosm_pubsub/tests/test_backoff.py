"""
Test backoff policies.

"""
from hamcrest import assert_that, calling, equal_to, is_, raises
from unittest.mock import patch

from microcosm_pubsub.backoff import ExponentialBackoffPolicy, NaiveBackoffPolicy
from microcosm_pubsub.errors import SkipMessage
from microcosm_pubsub.message import SQSMessage


def test_default_timeout():
    message = SQSMessage(
        None, None, None, None, None,
        approximate_receive_count=1,
    )
    backoff_policy = NaiveBackoffPolicy(42, -1)

    assert_that(
        backoff_policy.compute_backoff_timeout(message, None),
        is_(equal_to(42)),
    )


def test_message_timeout():
    message = SQSMessage(
        None, None, None, None, None,
        approximate_receive_count=1,
    )
    backoff_policy = NaiveBackoffPolicy(42, -1)

    assert_that(
        backoff_policy.compute_backoff_timeout(message, 77),
        is_(equal_to(77)),
    )

def test_skip_past_max_attempts():
    backoff_policy = NaiveBackoffPolicy(33, 5)

    message = SQSMessage(
        None, None, None, None, None,
        approximate_receive_count=4,
    )

    assert_that(
        backoff_policy.compute_backoff_timeout(message, None),
        is_(equal_to(33)),
    )

    message = SQSMessage(
        None, None, None, None, None,
        approximate_receive_count=6,
    )

    assert_that(
        calling(backoff_policy.compute_backoff_timeout).with_args(message, None),
        raises(SkipMessage),
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
