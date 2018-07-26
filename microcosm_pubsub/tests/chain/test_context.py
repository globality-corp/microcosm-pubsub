from hamcrest import (
    assert_that,
    calling,
    raises,
)

from microcosm_pubsub.chain.context import SafeContext


class TestChain:

    def test_cannot_overwrite(self):
        context = SafeContext()
        context["arg"] = 20
        assert_that(calling(context.update).with_args(arg=21), raises(ValueError))
