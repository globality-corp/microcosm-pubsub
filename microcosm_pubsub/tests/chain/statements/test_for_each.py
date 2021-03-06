from hamcrest import assert_that, equal_to, is_

from microcosm_pubsub.chain import Chain
from microcosm_pubsub.chain.statements import for_each


def test_for_each():
    chain = Chain(
        for_each("item").in_("items").do(
            Chain(lambda item: item.upper()),
        ),
        Chain(lambda item_list: list(reversed(item_list))),
    )

    assert_that(
        chain(items=["a", "b", "c"]),
        is_(equal_to(["C", "B", "A"])),
    )


def test_for_each_simplified():
    chain = Chain(
        for_each("item").in_("items").do(
            lambda item: item.upper(),
        ).as_("lst"),
        lambda lst: list(reversed(lst)),
    )

    assert_that(
        chain(items=["a", "b", "c"]),
        is_(equal_to(["C", "B", "A"])),
    )


def test_for_each_when():
    chain = Chain(
        for_each("item").in_("items").do(
            lambda item: None if item % 2 else item,
        ).when_not_none(),
    )

    assert_that(
        chain(items=[0, 1, 2, 3, 4, 5]),
        is_(equal_to([0, 2, 4])),
    )
