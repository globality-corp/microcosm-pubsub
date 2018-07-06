"""
Fluent decorators for methods.

"""

from marshmallow import ValidationError
from functools import wraps
from microcosm_flask.operations import Operation

from microcosm_pubsub.conventions import created


def _get_from_kwargs_or_component(component, key, kwargs):
    if key in kwargs:
        return kwargs[key]
    value = getattr(component, key, None)
    if value is not None:
        return value
    raise ValidationError(f"publish requires {key} argument or component.{key} definition")


def _create_default_media_type(component, **kwargs):
    ns = _get_from_kwargs_or_component(component, "ns", kwargs)
    return created(ns.subject)


def _create_default_uri_extractor(
    component,
    operation,
    uri_string_args=None,
    **kwargs,
):
    ns = _get_from_kwargs_or_component(component, "ns", kwargs)
    if uri_string_args is None:
        uri_string_args = dict()
        identifier_key = _get_from_kwargs_or_component(component, "identifier_key", kwargs)
        uri_string_args[identifier_key] = lambda model: str(model.id)

    return lambda model: ns.url_for(
        operation,
        **{key: str(extractor(model)) for key, extractor in uri_string_args.items()}
    )


def publish(
    producer_key="sns_producer",
    media_type=None,
    default_uri=True,
    uri_string_args=None,
    message_params=None,
    operation=None,
    **kwargs,
):
    """
    A method decorator generator that publish pubsub messages.

    Simple examples:

    def __init__(self, graph):
        self.ns = Namespace(subject="company")
        self.identifier_key = company_id

    @publish()
    def create(**kwargs):
        return company(id="ID")
    produce:
        application/vnd.globality.pubsub._.created.company message
        with: {'uri': '/api/v1/company/ID'}

    @publish(media_type=changed("Company"))
    def create(**kwargs):
        return company(id="ID")
    produce:
        application/vnd.globality.pubsub._.changed.company message
        with: {'uri': '/api/v1/company/ID'}

    @publish(uri_string_args={"company_id": lambda company: company.id, "limit": lambda company: 1})
        return company(id="ID")
    produce:
        application/vnd.globality.pubsub._.created.company message
        with: {'uri': '/api/v1/company/ID?limit=1'}

    Customization:

    :producer_key    - key of the component producer to use.
                       Fetch the producer with component.producer_key or component.graph.producer_key.
    :media_type      - the message media type. If is not specified, will try to create: created(component.ns.subject)
    :default_uri     - boolean, should autogenerate uri query string arg. If set to true,
                       Uses operation argument, component.identifier_key and function_result.id to generate uri
    :uri_string_args - Allow to specify query args to use with the default_uri instead of the default identifier_key.
                       Passed as a dictionary of {arg_name: lambda function_result: action}
                       For example: uri_string_args={
                           company_event_id: lambda company_event: company_event.id
                           min_clock: lambda company_event: company_event.clock
                        }
    :message_params  - Allow to specify additional message args to use with the default_uri.
                       Passed as a dictionary of {arg_name: lambda function_result: action}
                       For example: uri_string_args={ company_id: lambda company_event: company_event.company_id }
                       If default_uri is set to false, allows to pass uri argument for non standard uris
                       For example: uri_string_args={ uri: lambda resource: resource["_links"]["self"]["href"] }
                       It is recommended to set both media_type and message_params toghether.
    :ns              - If ns is set to true or if media_type is unspecified,
                       allows to specify any ns. Useful if component.ns is not set.
    :identifier_key  - If default_uri is set to true, allows to specify any identifier_key.
                       The value will still be model.id
                       Useful if component.identifier_key is not set or when setting the ns arg.
    :operation       - microcosm_flask.operations.Operation to use for autogenerating the default_uri.
                       Default is Operation.Retrieve.
                       It is recommended to set both operation and uri_string_args toghether.

    """
    def decorator(func):
        if default_uri and message_params and "uri" in message_params:
            raise ValidationError("Cannot pass both uri extractor and while default_uri set to true")
        operation_ = operation or Operation.Retrieve

        @wraps(func)
        def decorate(component, *func_args, **func_kwargs):
            sns_producer = getattr(component, producer_key, None)
            if sns_producer is None:
                sns_producer = getattr(component.graph, producer_key)
            media_type_to_publish = media_type or _create_default_media_type(component, **kwargs)

            extractors = dict(message_params) if message_params else dict()
            if default_uri:
                uri = _create_default_uri_extractor(component, operation_, uri_string_args, **kwargs)
                extractors["uri"] = uri

            model = func(component, *func_args, **func_kwargs)
            publish_kwargs = {key: extractor(model) for key, extractor in extractors.items()}

            sns_producer.produce(media_type_to_publish, **publish_kwargs)

            return model

        return decorate

    return decorator
