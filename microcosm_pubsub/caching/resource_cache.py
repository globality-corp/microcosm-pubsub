from microcosm.api import defaults, typed
from microcosm.config.types import boolean

from microcosm_pubsub.caching.memcached import MemcachedCache


@defaults(
    enabled=typed(boolean, default_value=False),
    host="localhost",
    port=typed(int, default_value=11211),
)
def configure_resource_cache(graph):
    """
    Configure the resource cache which will be used by URIHandlers
    and derived pubsub daemon handlers.

    """
    if not graph.config.resource_cache.enabled:
        return None

    kwargs = dict(
        host=graph.config.resource_cache.host,
        port=graph.config.resource_cache.port,
    )
    if graph.metadata.testing:
        kwargs.update(dict(testing=True))

    return MemcachedCache(**kwargs)
