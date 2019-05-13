from hamcrest import assert_that, equal_to, is_

from microcosm_pubsub.chain import Chain
from microcosm_pubsub.chain.decorators import binds, extracts


class TestChain:

    def test_chain_returns_value(self):
        chain = Chain(
            lambda: 100,
            lambda: 200,
        )
        assert_that(
            chain(),
            is_(equal_to(200)),
        )

    def test_chain_functions_can_pass_variables(self):
        chain = Chain(
            lambda arg: arg,
        )
        assert_that(
            chain(arg=200),
            is_(equal_to(200)),
        )

    def test_chain_functions_has_access_to_context(self):
        chain = Chain(
            lambda context: context.arg,
        )
        assert_that(
            chain(arg=200),
            is_(equal_to(200)),
        )

    def test_chain_functions_can_use_decorators(self):
        chain = Chain(
            extracts("arg")(lambda: 20),
            lambda arg: arg * 10,
        )
        assert_that(
            chain(),
            is_(equal_to(200)),
        )

    def test_chain_in_a_chain(self):
        chain = Chain(
            extracts("arg")(lambda: 20),
            Chain(
                lambda arg: arg * 10,
            ),
        )
        assert_that(
            chain(),
            is_(equal_to(200)),
        )

    def test_chain_link_with_multiple_decorators(self):

        @extracts("param")
        class Extractor:
            def __call__(self):
                return 20

        class Transformer:
            @extracts("res")
            def transform(self, arg):
                return arg * 10

        chain = Chain(
            Extractor(),
            binds(param="arg")(Transformer().transform),
            lambda res: res,
        )
        assert_that(
            chain(),
            is_(equal_to(200)),
        )
