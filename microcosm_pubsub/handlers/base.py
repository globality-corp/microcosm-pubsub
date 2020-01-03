"""
Base pubsub handler

"""
from abc import ABCMeta
from re import search

from inflection import titleize
from microcosm.errors import LockedGraphError, NotBoundError

from microcosm_pubsub.constants import DEFAULT_RESOURCE_CACHE_TTL
from microcosm_pubsub.conventions.lifecycle import LifecycleChange


def resource_cache_whitelist_callable(media_type, uri):
    """
    Default resource cache whitelist callable implementation.

    Only whitelists under the following conditions:
    * verb is created
    * resource is an event

    """
    if not media_type:
        # Nb. likely to happen with mocks and tests.
        return False

    return all((
        search(r"/[a-z]+_event/", uri),
        search(r".{}.".format(LifecycleChange.Created), media_type),
    ))


class PubSubHandler(metaclass=ABCMeta):
    """
    Base handler for pubsub messages.

    There are five expected outcomes for this handler:
      - Raising an error (e.g. a bug)
      - Skipping the handler.
      - Handling the message.
      - Ignoring the message.
      - Raising a nack to reprocess at a later date (e.g. as a protection against race conditions)

    The middle three cases are all handled here with the expectation that we produce *one* INFO-level
    log per message processed (unless an error/nack is raised).

    """
    def __init__(
        self,
        graph,
        retry_nack_timeout=1,
        resource_nack_timeout=1,
        resource_cache_enabled=True,
        resource_cache_ttl=DEFAULT_RESOURCE_CACHE_TTL,
        resource_cache_whitelist_callable=resource_cache_whitelist_callable,
    ):
        self.opaque = graph.opaque
        self.retry_nack_timeout = retry_nack_timeout
        self.resource_nack_timeout = resource_nack_timeout
        self.resource_cache_ttl = resource_cache_ttl
        self.resource_cache = self.get_resource_cache(graph) if resource_cache_enabled else None
        self.resource_cache_whitelist_callable = resource_cache_whitelist_callable

    @property
    def name(self):
        return titleize(self.__class__.__name__)

    @property
    def nack_if_not_found(self):
        return True

    @property
    def resource_type(self):
        return dict

    def get_resource_cache(self, graph):
        try:
            return graph.resource_cache
        except (LockedGraphError, NotBoundError):
            # Nb. if resource cache is globally disabled, will not be bound
            return None

    def _pre_handle(self, message, uri):
        self.on_call(message, uri)

        skip_reason = self.get_reason_to_skip(message, uri)
        if skip_reason is not None:
            self.on_skip(message, uri, skip_reason)
            return False

    def __call__(self, message):
        uri = message["uri"]
        self._pre_handle(message, uri)

        # XXX: `None` is for backwards-compatibility with URIHandler signature
        # Proper separation will come if we move forward with no-fetch handling
        if self.handle(message, uri, None):
            self.on_handle(message, uri, None)
            return True
        else:
            self.on_ignore(message, uri, None)
            return False

    def on_call(self, message, uri):
        self.logger.debug(
            "Starting {handler}",
            extra=dict(
                handler=self.name,
                uri=uri,
            ),
        )

    def on_skip(self, message, uri, reason):
        self.logger.info(
            "Skipping {handler} because {reason}",
            extra=dict(
                handler=self.name,
                reason=reason,
                uri=uri,
            ),
        )

    def on_handle(self, message, uri, resource):
        self.logger.debug(
            "Handled {handler}",
            extra=dict(
                handler=self.name,
                uri=uri,
            ),
        )

    def on_ignore(self, message, uri, resource):
        self.logger.info(
            "Ignored {handler}",
            extra=dict(
                handler=self.name,
                uri=uri,
            ),
        )

    def get_reason_to_skip(self, message, uri):
        """
        Some messages carry enough context that we can avoid resolving the URI entirely.

        """
        return None

    def get_headers(self, message):
        """
        Generate headers to pass to downstream services.

        """
        return self.opaque.as_dict()

    def handle(self, message, uri, resource):
        return True
