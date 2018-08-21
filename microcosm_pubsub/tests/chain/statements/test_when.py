from hamcrest import (
    assert_that,
    equal_to,
    is_,
)

from microcosm_pubsub.chain import Chain
from microcosm_pubsub.chain.statements import (
    when,
)


def test_when():
    chain = Chain(
        when("arg").then(
            Chain(lambda: 200),
        ).otherwise(
            Chain(lambda: 400),
        ),
    )
    assert_that(
        chain(arg=True),
        is_(equal_to(200)),
    )
    assert_that(
        chain(arg=False),
        is_(equal_to(400)),
    )


def test_when_simplified():
    chain = Chain(
        when("arg").then(
            lambda: 200,
        ).otherwise(
            lambda: 400,
        ),
    )
    assert_that(
        chain(arg=True),
        is_(equal_to(200)),
    )
    assert_that(
        chain(arg=False),
        is_(equal_to(400)),
    )


def test_empty_when():
    chain = Chain(
        when("arg"),
    )
    assert_that(
        chain(arg=True),
        is_(equal_to(None)),
    )
    assert_that(
        chain(arg=False),
        is_(equal_to(None)),
    )
