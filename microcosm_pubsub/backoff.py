"""
Consumer backoff policy

"""
from abc import ABCMeta, abstractmethod
from random import randint


# We cannot go over 12 hours
MAX_BACKOFF_TIMEOUT = 60 * 60 * 12


class BackoffPolicy(metaclass=ABCMeta):

    @abstractmethod
    def compute_backoff_timeout(self, message, message_timeout):
        pass

    @classmethod
    def choose_backoff_policy(cls, name):
        for subclass in cls.__subclasses__():
            if subclass.__name__ == name:
                return subclass
        raise Exception(f"No backoff policy configured with class name: {name}")


class NaiveBackoffPolicy(BackoffPolicy):
    """
    Naive backoff policy.

    Uses a fixed timeout from the message or system default.

    """
    def __init__(self, message_retry_visibility_timeout_seconds, **kwargs):
        self.message_retry_visibility_timeout_seconds = message_retry_visibility_timeout_seconds

    def compute_backoff_timeout(self, message, message_timeout):
        backoff_timeout = message_timeout or self.message_retry_visibility_timeout_seconds
        # we can only set integer timeouts
        return int(backoff_timeout) if backoff_timeout is not None else backoff_timeout


class ExponentialBackoffPolicy(BackoffPolicy):
    """
    Exponential backoff policy.

    Uses a timeout scaled between 1 and an exponential limit.

    """
    def __init__(self, **kwargs):
        pass

    def compute_backoff_timeout(self, message, message_timeout):
        if message_timeout is not None:
            return message_timeout

        # exponential backoff means that on the Cth failure, timeout is maximized at N=2^C - 1
        upper = 2**message.approximate_receive_count - 1

        # randomly select a timeout between 1..N; note that proper exponential backoff uses 0..N
        scaling_factor = randint(1, upper)

        return min(scaling_factor, MAX_BACKOFF_TIMEOUT)


class ExponentialBackoffBaseJitterPolicy(BackoffPolicy):
    """
    Exponential backoff jitter with base policy.

    Uses a timeout scaled between number or retry and an exponential limit.

    """
    def __init__(self, **kwargs):
        pass

    def compute_backoff_timeout(self, message, message_timeout):
        if message_timeout is not None:
            return message_timeout

        # Slow down exponentially, but add a random equal jitter to avoid thundering herd
        # we use the base as the number of times the message has been received (1 second for each minimum wait)
        # the time we wait is between half the base + random number between 0 and half the scaling factor
        # this means we wait:
        #   0-1 second on the original message (this never happens),
        #   1-2 second for the first retry,
        #   2-4 seconds for the second retry,
        #   3-7 seconds for the third retry,
        #   4-12 seconds for the fourth retry,
        #   5-21 seconds for the fifth retry, etc.
        base = message.approximate_receive_count
        upper = int(2**base)
        scaling_factor = randint(base, upper)

        return min(int(base + randint(0, int(scaling_factor/2))), MAX_BACKOFF_TIMEOUT)
