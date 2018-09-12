"""
Message context.

"""

from microcosm.api import defaults, typed


def sqs_message_context(message_dct, **kwargs):
    context = message_dct.get("opaque_data", dict())

    # If there is a uri add it to the context
    if message_dct.get("uri"):
        context["uri"] = message_dct.get("uri")

    context.update(**kwargs)
    return context


@defaults(
    enable_ttl=typed(bool, default_value=True),
    initial_ttl=typed(int, default_value=32),
)
def configure_sqs_message_context(graph):
    """
    Configure the message context function which controls what data you want to associate
    with your daemon handler context, e.g. some combination of keys from the message or
    additional metadata fetched from elsewhere.

    Usage:
        graph.message_context()

    """
    return sqs_message_context
