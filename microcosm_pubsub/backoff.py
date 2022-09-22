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
        raise Exception("No backoff policy configured with class name: {}".format(name))


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
    def __init__(self, backoff_factor=2.0, **kwargs):
        self.backoff_factor = backoff_factor

    def compute_backoff_timeout(self, message, message_timeout):
        # exponential backoff means that on the Cth failure, timeout is maximized at N=backoff_factor^C - 1
        upper = int(self.backoff_factor**message.approximate_receive_count) - 1

        # randomly select a timeout between 1..N; note that proper exponential backoff uses 0..N
        scaling_factor = self.randint(1, upper)

        return min(scaling_factor, MAX_BACKOFF_TIMEOUT)

    def randint(self, lower, upper):
        """
        Test friendly randomization.

        """
        return randint(lower, upper)
