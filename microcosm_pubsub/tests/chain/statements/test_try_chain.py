from hamcrest import (
    assert_that,
    calling,
    equal_to,
    is_,
    raises,
)

from microcosm_pubsub.chain import Chain
from microcosm_pubsub.chain.statements import try_chain


def test_try_chain():
    def function(exception):
        if exception is not None:
            raise exception()
        return 400

    chain = Chain(
        try_chain(
            Chain(function),
        ).catch(
            ValueError, Chain(lambda: 501),
        ).catch(
            KeyError, Chain(lambda: 502),
        )
    )
    assert_that(
        chain(exception=None),
        is_(equal_to(400)),
    )
    assert_that(
        chain(exception=ValueError),
        is_(equal_to(501)),
    )
    assert_that(
        calling(chain).with_args(exception=ArithmeticError),
        raises(ArithmeticError),
    )


def test_try_chain_simplified():
    def function(exception):
        if exception is not None:
            raise exception()
        return 400

    chain = Chain(
        try_chain(
            function,
        ).catch(ValueError).then(
            lambda: 501,
        ).catch(KeyError).then(
            lambda: 502,
        ),
    )
    assert_that(
        chain(exception=None),
        is_(equal_to(400)),
    )
    assert_that(
        chain(exception=ValueError),
        is_(equal_to(501)),
    )
    assert_that(
        calling(chain).with_args(exception=ArithmeticError),
        raises(ArithmeticError),
    )


def test_try_other():
    def function(exception):
        return 400

    chain = Chain(
        try_chain(
            Chain(function),
        ).otherwise(
            Chain(lambda: 200),
        ),
    )
    assert_that(
        chain(exception=None),
        is_(equal_to(200)),
    )


def test_try_other_simplified():
    def function(exception):
        return 400

    chain = Chain(
        try_chain(
            function,
        ).otherwise(
            lambda: 200,
        ),
    )
    assert_that(
        chain(exception=None),
        is_(equal_to(200)),
    )


def test_try_complex_chain():
    def function1(exception):
        if exception is ValueError:
            raise exception()
        return True

    def function2(exception):
        if exception is ArithmeticError:
            raise exception()
        return True

    chain = Chain(
        try_chain(
            function1,
            function2,
        ).catch(ValueError).then(
            lambda: 501,
        ).catch(ArithmeticError).then(
            lambda: 502,
        ).otherwise(
            Chain(lambda: 200),
        ),
    )
    assert_that(
        chain(exception=ValueError),
        is_(equal_to(501)),
    )
    assert_that(
        chain(exception=ArithmeticError),
        is_(equal_to(502)),
    )

    assert_that(
        chain(exception=None),
        is_(equal_to(200)),
    )
