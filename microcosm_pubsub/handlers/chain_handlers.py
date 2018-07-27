from microcosm_pubsub.chain import Chain
from microcosm_pubsub.handlers.uri_handler import URIHandler
from abc import ABCMeta, abstractmethod


class ChainHandler(metaclass=ABCMeta):
    """
    Resolve a chain on call. Pass to the chain the message to the chain.

    """
    @abstractmethod
    def get_chain(self):
        pass

    def __call__(self, message):
        return Chain(self.get_chain())(message=message)


class ChainURIHandler(URIHandler, metaclass=ABCMeta):
    """
    Base handler for URI-driven events based on URIHandler
    Resolve a chain on handle.
    Pass to the chain the message and the fetched resource.

    """
    @abstractmethod
    def get_chain(self):
        pass

    @property
    def resource_name(self):
        return "resource"

    def handle(self, message, uri, resource):
        kwargs = dict(message=message)
        kwargs[self.resource_name] = resource
        chain = self.get_chain()
        chain(**kwargs)

        return True
