from dataclasses import dataclass, field
from os import environ
from typing import Any, Dict, Optional
from unittest.mock import MagicMock

from microcosm.decorators import defaults
from microcosm.errors import NotBoundError
from microcosm.object_graph import ObjectGraph
from microcosm_logging.decorators import logger


try:
    import sentry_sdk
    from sentry_sdk.utils import BadDsn
except ImportError:
    sentry_sdk = None  # type:ignore


DEFAULT_TAG_MAPPING = {
    "X-Request-Id": "x-request-id",
    "message_id": "message-id",
    "media_type": "media-type",
    "span_name": "span",
    "uri": "uri",
    "handler": "handler",
}
DEFAULT_USER_ID_KEY = "X-Request-User"


@dataclass
class SentryConfigPubsub:
    dsn: Optional[str] = None
    enabled: bool = False
    client: Optional[Any] = None
    tag_mapping: Dict[str, str] = field(default_factory=dict)
    user_id_key: Optional[str] = None


def default_before_send(event: Dict[str, Any], hint) -> Dict[str, Any]:
    """
    Before sending a event to sentry scrub all values that are non-uuid data from the event.
    Allow python internals such as <method >, <func > etc. and self, cls.

    """
    for exception in event["exception"]["values"]:
        for frame in exception["stacktrace"]["frames"]:
            for var_name, value in frame["vars"].items():
                if not value:
                    continue
                if var_name in ["self", "cls"]:
                    continue
                elif isinstance(value, str):
                    if value.startswith("<") and value.endswith(">"):
                        continue
                    elif var_name.endswith("_id"):
                        continue
                    elif value.startswith("http"):
                        continue
                elif isinstance(value, dict):
                    for key in value.keys():
                        if key.endswith("_id"):
                            continue
                        value[key] = "<redacted>"
                    continue
                frame["vars"][var_name] = "<redacted>"
    return event


@defaults(
    enabled=False,
    dsn=None,
    custom_tags_mapping=None,
    custom_user_id=None,
)
@logger
def configure_sentry_pubsub(graph: ObjectGraph) -> SentryConfigPubsub:
    enabled, dsn, sentry = False, None, None
    tag_mapping, custom_user_id_key = DEFAULT_TAG_MAPPING, DEFAULT_USER_ID_KEY

    if graph.config.sentry_logging_pubsub.enabled and graph.config.sentry_logging_pubsub.dsn and sentry_sdk:
        enabled = graph.config.sentry_logging_pubsub.enabled
        dsn = graph.config.sentry_logging_pubsub.dsn

        try:
            before_send_func = graph.sentry_before_send
        except NotBoundError:
            before_send_func = default_before_send

        build_info = getattr(graph.config, "build_info_convention", None)
        sentry_kwargs = dict(
            dsn=dsn,
            server_name=f"{graph.metadata.name}_daemon",
            release=getattr(build_info, "sha1", None),
            environment=environ.get("MICROCOSM_ENVIRONMENT") or "undefined",
            before_send=before_send_func,
        )

        if graph.metadata.testing:
            sentry = MagicMock(**sentry_kwargs)
        else:
            try:
                sentry = sentry_sdk.init(**sentry_kwargs)
            except BadDsn:
                enabled, dsn, sentry = False, None, None
                configure_sentry_pubsub.logger.error("Bad DSN value set")
    if graph.config.sentry_logging_pubsub.custom_tags_mapping:
        tag_mapping.update(graph.config.sentry_logging_pubsub.custom_tags)
    if graph.config.sentry_logging_pubsub.custom_user_id:
        custom_user_id_key = graph.config.sentry_logging_pubsub.custom_user_id

    return SentryConfigPubsub(
        dsn=dsn,
        enabled=enabled,
        client=sentry,
        tag_mapping=tag_mapping,
        user_id_key=custom_user_id_key,
    )
