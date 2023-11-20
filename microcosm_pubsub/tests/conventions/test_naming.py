"""
Naming tests.

"""
import pytest

from microcosm_pubsub.conventions import make_media_type


@pytest.mark.parametrize(
    "args, kwargs, expected",
    [
        (("foo",), dict(), "application/vnd.globality.pubsub._.created.foo"),
        (("foo",), dict(public=True), "application/vnd.globality.pubsub.created.foo"),
        (("foo",), dict(organization="example"), "application/vnd.example.pubsub._.created.foo"),
        (("FooBar",), dict(), "application/vnd.globality.pubsub._.created.foo_bar"),
        (("FooBar.ThisThat",), dict(), "application/vnd.globality.pubsub._.created.foo_bar.this_that"),
    ]
)
def test_make_media_type(args, kwargs, expected):
    """
    Media type construction should generate the correct type strings.

    """
    assert make_media_type(*args, **kwargs) == expected
