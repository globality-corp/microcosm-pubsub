from hamcrest import assert_that, equal_to

from microcosm_pubsub.chain import Chain
from microcosm_pubsub.chain.statements.call import call


def test_local_call_wrapper():
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
