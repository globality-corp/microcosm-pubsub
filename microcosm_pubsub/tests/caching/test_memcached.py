"""
Unit-tests for memcached cache backend.

"""
from hamcrest import assert_that, equal_to, is_
from nose.plugins.attrib import attr
from parameterized import parameterized

from microcosm_pubsub.caching.memcached import MemcachedCache


@attr("caching")
class TestMemcachedCache:

    def setup(self):
        self.cache = MemcachedCache(testing=True)

    @parameterized([
        ("key", "string-value"),
        ("http://globality.io/resource/98f6c9ec-043f-4997-b98d-c72b5088c204", dict(
            foo="bar",
            bar=1,
            baz=123.45,
            nested=dict(nested_key="nested_value"),
            lst=["1", "2", "3"],
        )),
    ])
    def test_set_and_get_value(self, key, value):
        self.cache.set("key", value)

        assert_that(
            self.cache.get("key"),
            is_(equal_to(value)),
        )
