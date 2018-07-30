"""
Uri Handler base classes.

"""
from abc import ABCMeta
from inflection import titleize

from requests import codes, get

from microcosm_pubsub.errors import Nack


class URIHandler:
    """
    Base handler for URI-driven events.

    As a general rule, we want PubSub events to convey the URI of a resource that was created
    (because resources are ideally immutable state). In this case, we want asynchronous workers
    to query the existing URI to get more information (and to handle race conditions where the
    message was delivered before the resource was committed.)

    There are five expected outcomes for this handler:
      - Raising an error (e.g. a bug)
      - Skipping the handlers (because the pubsub message carried enough information to bypass processing)
      - Handling the message after fetching the resource by URI
      - Ignore the message after fetching the resource by URI
      - Raising a nack (e.g. because the resource was not committed yet)

    The middle three cases are all handled here with the expectation that we produce *one* INFO-level
    log per message processed (unless an error/nack is raised).

    """
    __metaclass__ = ABCMeta

    def __init__(self, graph, nack_timeout=1):
        self.sqs_message_context = graph.sqs_message_context
        self.nack_timeout = nack_timeout

    @property
    def name(self):
        return titleize(self.__class__.__name__)

    @property
    def nack_if_not_found(self):
        return True

    @property
    def resource_type(self):
        return dict

    def __call__(self, message):
        uri = message["uri"]
        self.on_call(message, uri)

        skip_reason = self.get_reason_to_skip(message, uri)
        if skip_reason is not None:
            self.on_skip(message, uri, skip_reason)
            return False

        resource = self.get_resource(message, uri)

        if self.handle(message, uri, resource):
            self.on_handle(message, uri, resource)
            return True
        else:
            self.on_ignore(message, uri, resource)
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

    def get_resource(self, message, uri):
        """
        Mock-friendly URI getter.

        Passes message context.

        """
        headers = self.sqs_message_context(message)
        response = get(uri, headers=headers)
        if response.status_code == codes.not_found and self.nack_if_not_found:
            raise Nack(self.nack_timeout)
        response.raise_for_status()
        body = response.json()
        if isinstance(body, self.resource_type):
            return body
        return self.resource_type(**body)

    def handle(self, message, uri, resource):
        return True
