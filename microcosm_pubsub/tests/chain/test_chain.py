from hamcrest import (
    assert_that,
    calling,
    equal_to,
    is_,
    raises,
)
from marshmallow import ValidationError

from microcosm_pubsub.chain import Chain
from microcosm_pubsub.chain.decorators import extracts


class TestChain:

    def test_chain_returns_value(self):
        chain = Chain(
            lambda: 100,
            lambda: 200,
        )
        assert_that(
            chain.resolve(),
            is_(equal_to(200)),
        )

    def test_chain_functions_can_pass_variables(self):
        chain = Chain(
            lambda arg: arg,
        )
        assert_that(
            chain.resolve(arg=200),
            is_(equal_to(200)),
        )

    def test_chain_functions_has_access_to_context(self):
        chain = Chain(
            lambda context: context["arg"],
        )
        assert_that(
            chain.resolve(arg=200),
            is_(equal_to(200)),
        )

    def test_chain_functions_can_use_decorators(self):
        chain = Chain(
            extracts("arg")(lambda: 20),
            lambda arg: arg * 10,
        )
        assert_that(
            chain.resolve(),
            is_(equal_to(200)),
        )

    def test_cannot_override_value(self):
        chain = Chain(
            extracts("arg")(lambda: 20),
            extracts("arg")(lambda: 21),
        )
        assert_that(calling(chain.resolve), raises(ValidationError))

    def test_chain_in_a_chain(self):
        chain = Chain(
            extracts("arg")(lambda: 20),
            Chain(
                lambda arg: arg * 10,
            ),
        )
        assert_that(
            chain.resolve(),
            is_(equal_to(200)),
        )
