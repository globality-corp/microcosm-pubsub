"""
Message context.

"""


def sqs_message_context(message):
    # NB This is the simplest possible idea for associated context with a handler funcion.
    # In the future it would make sense to make this function configurable and
    # add some parameters to control additional behavior.
    return message.get("opaque_data", dict())


def configure_sqs_message_context(graph):
    """
    Configure the message context function which controls what data you want to associate
    with your daemon handler context, e.g. some combination of keys from the message or
    additional metadata fetched from elsewhere.

    Usage:
        graph.message_context()
    """

    return sqs_message_context
