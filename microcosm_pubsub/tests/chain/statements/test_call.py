from hamcrest import (
    assert_that,
    calling,
    equal_to,
    raises,
)

from microcosm_pubsub.chain import Chain
from microcosm_pubsub.chain.statements.call import call


class TestLocalCallWrapper:
    def two_calls_of_same_function(self):
        def double_it(input):
            return 2 * input

        def multiply(double_one, double_three):
            return double_one * double_three

        chain = Chain(
            call(double_it).with_args(input="one").as_("double_one"),
            call(double_it).with_args(input="three").as_("double_three"),
            multiply,
        )

        assert_that(
            chain(one=1, three=3),
            equal_to(12),
        )

    def mix_parent_child_context(self):
        # `sum_base` will come from the parent context
        # `addend` will be defined using `call`
        def sum(sum_base, addend):
            return sum_base + addend

        def noop(twelve):
            return twelve

        chain = Chain(
            call(sum).with_args(addend="two").as_("twelve"),
            noop,
        )

        assert_that(
            chain(sum_base=10, two=2),
            equal_to(12),
        )

    def error_on_overwrite_parent_context(self):
        # the kwargs in `with_args` overshadows an existing key on the context
        chain = Chain(
            call(sum).with_args(input="another_word").as_("twelve"),
        )

        assert_that(
            calling(chain).with_args(input="hello", another_word="world"),
            raises(ValueError),
        )
