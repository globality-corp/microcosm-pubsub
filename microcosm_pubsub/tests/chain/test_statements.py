from hamcrest import (
    assert_that,
    calling,
    equal_to,
    is_,
    raises,
)

from microcosm_pubsub.chain import Chain
from microcosm_pubsub.chain.statements import extract_, if_, switch_, try_


class TestStatements:

    def test_extract_(self):
        chain = Chain(
            extract_("param", "arg"),
            lambda param: param,
        )
        assert_that(
            chain.resolve(arg=200),
            is_(equal_to(200)),
        )

    def test_extract_return_value(self):
        chain = Chain(
            extract_("param", "arg"),
        )
        assert_that(
            chain.resolve(arg=200),
            is_(equal_to(200)),
        )

    def test_extract_with_propery(self):
        chain = Chain(
            extract_("param", "arg", "data"),
        )
        assert_that(
            chain.resolve(arg=dict(data=200)),
            is_(equal_to(200)),
        )

    def test_extract_with_attribute(self):
        chain = Chain(
            extract_("param", "arg", "__class__"),
        )
        assert_that(
            chain.resolve(arg=dict()),
            is_(equal_to(dict)),
        )

    def test_if_(self):
        chain = Chain(
            if_("arg", then_=Chain(lambda: 200), else_=Chain(lambda: 400)),
        )
        assert_that(
            chain.resolve(arg=True),
            is_(equal_to(200)),
        )
        assert_that(
            chain.resolve(arg=False),
            is_(equal_to(400)),
        )

    def test_empty_if_(self):
        chain = Chain(
            if_("arg"),
        )
        assert_that(
            chain.resolve(arg=True),
            is_(equal_to(None)),
        )
        assert_that(
            chain.resolve(arg=False),
            is_(equal_to(None)),
        )

    def test_switch_(self):
        chain = Chain(
            switch_(
                "arg",
                cases=[
                    (True, Chain(lambda: 201)),
                    (False, Chain(lambda: 401)),
                ],
                else_=Chain(lambda: 400),
                yes=Chain(lambda: 200),
            ),
        )
        assert_that(
            chain.resolve(arg=True),
            is_(equal_to(201)),
        )
        assert_that(
            chain.resolve(arg=False),
            is_(equal_to(401)),
        )
        assert_that(
            chain.resolve(arg=None),
            is_(equal_to(400)),
        )
        assert_that(
            chain.resolve(arg="yes"),
            is_(equal_to(200)),
        )

    def test_empty_switch_(self):
        chain = Chain(
            switch_(
                "arg",
                yes=Chain(lambda: 200),
            ),
        )
        assert_that(
            chain.resolve(arg=None),
            is_(equal_to(None)),
        )

    def test_try_(self):
        def function(exception):
            if exception is not None:
                raise exception()
            return 400

        chain = Chain(
            try_(
                try_=Chain(function),
                except_=[
                    (ValueError, Chain(lambda: 501)),
                    (KeyError, Chain(lambda: 502)),
                ],
            ),
        )
        assert_that(
            chain.resolve(exception=None),
            is_(equal_to(400)),
        )
        assert_that(
            chain.resolve(exception=ValueError),
            is_(equal_to(501)),
        )
        assert_that(
            calling(chain.resolve).with_args(exception=ArithmeticError),
            raises(ArithmeticError),
        )

    def test_try_else_(self):
        def function(exception):
            return 400

        chain = Chain(
            try_(
                try_=Chain(function),
                else_=Chain(lambda: 200),
            ),
        )
        assert_that(
            chain.resolve(exception=None),
            is_(equal_to(200)),
        )
