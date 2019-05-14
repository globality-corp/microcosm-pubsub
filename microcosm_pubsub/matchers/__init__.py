"""
Hamcrest-style matchers for PubSub messages.

Note that this package depends on `PyHamcrest`, which is intentionally not an
installation dependency of `microcosm-pubsub` (but is a test dependnency).

Users are expected to install `PyHamcrest` as a test dependency and import
this package as part of their unit tests (and not their runtime).

"""
from microcosm_pubsub.matchers.message import (
    PublishedMessage,
    PublishedMessageMatcher,
    has_media_type,
    has_uri,
)
from microcosm_pubsub.matchers.publishing import (
    published,
    published_inanyorder,
    published_messages_for,
    published_nothing,
)


__all__ = [  # type: ignore
    has_media_type,
    has_uri,
    published,
    published_inanyorder,
    published_messages_for,
    published_nothing,
    PublishedMessage,
    PublishedMessageMatcher,
]
