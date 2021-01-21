from hamcrest import (
    assert_that,
    calling,
    contains_exactly,
    has_length,
    is_,
    raises,
)

from microcosm_pubsub.chain.context import SafeContext


class TestSafeContext:

    def test_can_set(self):
        context = SafeContext()
        context["arg"] = 20
        assert_that(context["arg"], is_(20))
        assert_that(context.arg, is_(20))

    def test_cannot_overwrite(self):
        context = SafeContext()
        context["arg"] = 20
        assert_that(
            calling(context.update).with_args(arg=21),
            raises(ValueError),
        )


class TestScopedSafeContext:

    def setup(self):
        self.parent = SafeContext()
        self.parent["arg"] = 20

        self.context = self.parent.local()

    def test_can_read_parent_arg(self):
        assert_that(self.context["arg"], is_(20))
        assert_that(self.context.arg, is_(20))

    def test_cannot_overwrite_parent_arg(self):
        assert_that(
            calling(self.context.update).with_args(arg=21),
            raises(ValueError),
        )

    def test_can_set_own_arg(self):
        self.context["arg2"] = 42
        assert_that(self.context["arg2"], is_(42))
        assert_that(self.context.arg2, is_(42))

    def test_cannot_overwrite_own_arg(self):
        self.context["arg2"] = 42
        assert_that(
            calling(self.context.update).with_args(arg2=21),
            raises(ValueError),
        )

    def test_union(self):
        self.context["arg2"] = 42

        assert_that(
            self.context,
            has_length(2),
        )
        assert_that(
            list(self.context),
            contains_exactly(
                "arg2",
                "arg",
            ),
        )
