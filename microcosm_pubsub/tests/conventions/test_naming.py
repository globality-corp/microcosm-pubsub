"""
Naming tests.

"""
from hamcrest import (
    assert_that,
    equal_to,
    is_
)
from microcosm_pubsub.conventions import make_media_type


def test_make_media_type():
    """
    Media type construction should generate the correct type strings.

    """
    cases = [
        (("foo",), dict(), "application/vnd.globality.pubsub._.created.foo"),
        (("foo",), dict(public=True), "application/vnd.globality.pubsub.created.foo"),
        (("foo",), dict(organization="example"), "application/vnd.example.pubsub._.created.foo"),
        (("FooBar",), dict(), "application/vnd.globality.pubsub._.created.foo_bar"),
        (("FooBar.ThisThat",), dict(), "application/vnd.globality.pubsub._.created.foo_bar.this_that"),
    ]

    def validate(args, kargs, expected):
        assert_that(make_media_type(*args, **kwargs), is_(equal_to(expected)))

    for args, kwargs, expected in cases:
        yield validate, args, kwargs, expected
