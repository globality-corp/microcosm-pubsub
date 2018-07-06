"""
Fluent decorators for methods.

"""
from functools import wraps


def publish(
    media_type=None,
    media_type_extractor=None,
    producer_key="sns_producer",
    **message_params,
):
    """
    A method decorator generator that publish pubsub messages.

    :producer_key     - Key of the component producer to use.
                        Fetch the producer with component.producer_key or component.graph.producer_key.
    :media_type       - The message media type.
                        Default is created(resource_name) where resource_name can be passed or fetched from component.ns
    :media_type_extractor - Allows to dynamically set the message type. Relevant if media_type is unspecified.
                        Passed as an extractor: lambda ctrl, res: action
                        Example:
                        * lambda ctrl, res: created(ctrl.subject)
    :**message_params - The message parameters. Passed as a dictionary of extractors:
                        {arg_name: lambda ctrl, res: action}
                        Examples:
                        * {uri: lambda ctrl, resource: resource["_links"]["self"]["href"]}
                        * {company_id: lambda ctrl, company_event: company_event.company_id}

    Simple examples:

    def uri_extractor_factory():
        return lambda component, model: f"/api/v1/company/{model.id}"

    def media_type_extractor():
        return lambda component, model: changed(component.subject)

    class Example:
        def __init__(self, graph):
            self.subject = "company"

        @publish(media_type=created(company), uri=uri_extractor_factory())
        def create(**kwargs):
            return company(id="ID")
        produce:
            application/vnd.globality.pubsub._.created.company message
            with: {'uri': '/api/v1/company/ID'}

        @publish(media_type_extractor=media_type_extractor(), uri=uri_extractor_factory())
        def create(**kwargs):
            return company(id="ID")
        produce:
            application/vnd.globality.pubsub._.changed.company message
            with: {'uri': '/api/v1/company/ID'}

        @publish(media_type=created(company), company_id=lambda component, company: company.id)
        def create(**kwargs):
            return company(id="ID")
        produce:
            application/vnd.globality.pubsub._.created.company message
            with: {'company_id': 'id'}

    """
    if media_type and media_type_extractor:
        raise TypeError("Cannot pass both media_type and media_type_extractor")
    if not media_type and not media_type_extractor:
        raise TypeError("Must pass either media_type or media_type_extractor")

    def decorator(func):
        @wraps(func)
        def decorate(component, *func_args, **func_kwargs):
            sns_producer = getattr(component, producer_key, None) or getattr(component.graph, producer_key)
            model = func(component, *func_args, **func_kwargs)
            media_type_ = media_type if media_type else media_type_extractor(component, model)
            publish_kwargs = {key: extractor(component, model) for key, extractor in message_params.items()}
            sns_producer.produce(media_type_, **publish_kwargs)
            return model
        return decorate
    return decorator
