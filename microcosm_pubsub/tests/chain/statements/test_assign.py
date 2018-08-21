from hamcrest import (
    assert_that,
    equal_to,
    is_,
)


from microcosm_pubsub.chain import Chain
from microcosm_pubsub.chain.statements import (
    assign,
    extract,
)


def test_extract():
    chain = Chain(
        extract("param", "arg"),
        lambda param: param,
    )
    assert_that(
        chain(arg=200),
        is_(equal_to(200)),
    )


def test_extractreturn_value():
    chain = Chain(
        extract("param", "arg"),
    )
    assert_that(
        chain(arg=200),
        is_(equal_to(200)),
    )


def test_extractwith_property():
    chain = Chain(
        extract("param", "arg", "data"),
    )
    assert_that(
        chain(arg=dict(data=200)),
        is_(equal_to(200)),
    )


def test_extractwith_property_simplified():
    chain = Chain(
        extract("param", "arg.data"),
    )
    assert_that(
        chain(arg=dict(data=200)),
        is_(equal_to(200)),
    )


def test_extractwith_attribute():
    chain = Chain(
        extract("param", "arg", "__class__"),
    )
    assert_that(
        chain(arg=dict()),
        is_(equal_to(dict)),
    )


def test_extractwith_attribute_simplified():
    chain = Chain(
        extract("param", "arg.__class__"),
    )
    assert_that(
        chain(arg=dict()),
        is_(equal_to(dict)),
    )


def test_assign_property():
    chain = Chain(
        assign("arg.data").to("param"),
    )
    assert_that(
        chain(arg=dict(data=200)),
        is_(equal_to(200)),
    )


def test_assign_attribute():
    chain = Chain(
        assign("arg.__class__").to("param"),
    )
    assert_that(
        chain(arg=dict()),
        is_(equal_to(dict)),
    )
