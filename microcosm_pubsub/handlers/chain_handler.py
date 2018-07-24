from microcosm_pubsub.chain import Chain
from microcosm_pubsub.handlers.uri_handler import URIHandler


class ChainHandler:
    def get_chain(self):
        raise NotImplementedError()

    def __call__(self, message):
        return Chain(
            self.get_chain(),
        ).resolve(
            message=message,
        )


class ChainURIHandler(URIHandler):
    def get_chain(self):
        raise NotImplementedError()

    def handle(self, message, uri, resource):
        return Chain(
            self.get_chain(),
        ).resolve(
            resource=resource,
            message=message,
        )
