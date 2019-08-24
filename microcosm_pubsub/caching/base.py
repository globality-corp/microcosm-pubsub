"""
Cache abstractions for use with PubSub resources.

"""
from abc import ABC, abstractmethod


class CacheBase(ABC):
    """
    A simple key-value cache interface.

    Used in the context of PubSub to cache API Resources.

    """
    @abstractmethod
    def get(self, key):
        pass

    @abstractmethod
    def set(self, key, value):
        pass
