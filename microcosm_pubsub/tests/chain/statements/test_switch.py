from hamcrest import (
    assert_that,
    equal_to,
    is_,
)

from microcosm_pubsub.chain import Chain
from microcosm_pubsub.chain.statements import (
    switch,
)


def test_switch():
    chain = Chain(
        switch("arg").case(
            True, Chain(lambda: 200),
        ).case(
            False, Chain(lambda: 400),
        ).otherwise(
            Chain(lambda: 500),
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
    assert_that(
        chain(arg=None),
        is_(equal_to(500)),
    )


def test_switch_simplified():
    chain = Chain(
        switch("arg").case(True).then(
            lambda: 200,
        ).case(False).then(
            lambda: 400,
        ).otherwise(
            lambda: 500,
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
    assert_that(
        chain(arg=None),
        is_(equal_to(500)),
    )


def test_empty_switch():
    chain = Chain(
        switch("arg"),
    )
    assert_that(
        chain(arg=None),
        is_(equal_to(None)),
    )
