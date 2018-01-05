"""
Consumer backoff policy

"""
from abc import ABCMeta, abstractmethod
from random import randint


# we cannot go over 12 hours
MAX_BACKOFF_TIMEOUT = 60 * 60 * 12


class BackoffPolicy(metaclass=ABCMeta):

    def __init__(self, visibility_timeout_seconds=None):
        self.visibility_timeout_seconds = visibility_timeout_seconds

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
    def compute_backoff_timeout(self, message, message_timeout):
        backoff_timeout = message_timeout or self.visibility_timeout_seconds
        # we can only set integer timeouts
        return int(backoff_timeout) if backoff_timeout is not None else backoff_timeout


class ExponentialBackoffPolicy(BackoffPolicy):
    """
    Exponential backoff policy.

    Uses a timeout scaled between 1 and an expontential limit.

    """
    def compute_backoff_timeout(self, message, message_timeout):
        # exponential backoff means that on the Cth failure, timeout is maximized at N=2^C - 1
        upper = 2**message.approximate_receive_count - 1

        # randomly select a timeout between 1..N; note that proper exponential backoff uses 0..N
        scaling_factor = self.randint(1, upper)

        return min(scaling_factor, MAX_BACKOFF_TIMEOUT)

    def randint(self, lower, upper):
        """
        Test friendly randomization.

        """
        return randint(lower, upper)
