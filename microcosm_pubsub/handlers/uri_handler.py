"""
Uri Handler base classes.

"""
from requests import codes, get

from microcosm_pubsub.errors import Nack
from microcosm_pubsub.handlers.base import PubSubHandler


class URIHandler(PubSubHandler):
    """
    Base handler for URI-driven events.

    As a general rule, we want PubSub events to convey the URI of a resource that was created
    (because resources are ideally immutable state). In this case, we want asynchronous workers
    to query the existing URI to get more information (and to handle race conditions where the
    message was delivered before the resource was committed.)

    We still have the same five expected outcomes described in the base classe.
    The only difference is that in this case skipping can happen either before fetching the resource
    (using `get_reason_to_skip`) or afterwards.

    """
    def __init__(self, graph, **kwargs):
        super().__init__(graph, **kwargs)
        self.fetch_resource = graph.config.sqs_message_dispatcher.fetch_uri_resource

    def __call__(self, message):
        uri = message["uri"]
        self._pre_handle(message, uri)

        resource = None
        # XXX: remove conditional and use base class instead post-POC
        if self.fetch_resource:
            resource = self.convert_resource(
                self.get_resource(message, uri),
            )

        if self.handle(message, uri, resource):
            self.on_handle(message, uri, resource)
            return True
        else:
            self.on_ignore(message, uri, resource)
            return False

    @property
    def nack_timeout(self):
        """Deprecated, use retry_nack_timeout"""
        return self.retry_nack_timeout

    def validate_changed_field(self, message, resource):
        if message.get('field_name') and self.nack_if_not_found:
            field_name = message["field_name"]
            new_value = message["new_value"]
            if resource.get(field_name) != new_value:
                raise Nack(self.resource_nack_timeout)

    def get_resource(self, message, uri):
        """
        Mock-friendly URI getter.

        Passes message context.

        """
        if self.resource_cache and self.resource_cache_whitelist_callable(
            media_type=message.get("mediaType"),
            uri=uri
        ):
            response = self.resource_cache.get(uri)
            if response:
                return response

        headers = self.get_headers(message)
        response = get(uri, headers=headers)
        if response.status_code == codes.not_found and self.nack_if_not_found:
            raise Nack(self.resource_nack_timeout)
        response.raise_for_status()
        response_json = response.json()

        self.validate_changed_field(message, response_json)

        if self.resource_cache and self.resource_cache_whitelist_callable(
            media_type=message.get("mediaType"),
            uri=uri,
        ):
            self.resource_cache.set(uri, response_json, ttl=self.resource_cache_ttl)

        return response_json

    def convert_resource(self, resource):
        if isinstance(self.resource_type, type) and isinstance(resource, self.resource_type):
            return resource
        return self.resource_type(**resource)
