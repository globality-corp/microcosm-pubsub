from hamcrest import (
    assert_that,
    calling,
    equal_to,
    is_,
    raises,
)

from microcosm_pubsub.chain import Chain
from microcosm_pubsub.chain.statements import extract, when, switch, try_chain


class TestStatements:

    def test_extract(self):
        chain = Chain(
            extract("param", "arg"),
            lambda param: param,
        )
        assert_that(
            chain(arg=200),
            is_(equal_to(200)),
        )

    def test_extractreturn_value(self):
        chain = Chain(
            extract("param", "arg"),
        )
        assert_that(
            chain(arg=200),
            is_(equal_to(200)),
        )

    def test_extractwith_propery(self):
        chain = Chain(
            extract("param", "arg", "data"),
        )
        assert_that(
            chain(arg=dict(data=200)),
            is_(equal_to(200)),
        )

    def test_extractwith_attribute(self):
        chain = Chain(
            extract("param", "arg", "__class__"),
        )
        assert_that(
            chain(arg=dict()),
            is_(equal_to(dict)),
        )

    def test_when(self):
        chain = Chain(
            when("arg", chain=Chain(lambda: 200), otherwise=Chain(lambda: 400)),
        )
        assert_that(
            chain(arg=True),
            is_(equal_to(200)),
        )
        assert_that(
            chain(arg=False),
            is_(equal_to(400)),
        )

    def test_empty_when(self):
        chain = Chain(
            when("arg"),
        )
        assert_that(
            chain(arg=True),
            is_(equal_to(None)),
        )
        assert_that(
            chain(arg=False),
            is_(equal_to(None)),
        )

    def test_switch(self):
        chain = Chain(
            switch(
                "arg",
                cases=[
                    (True, Chain(lambda: 201)),
                    (False, Chain(lambda: 401)),
                ],
                otherwise=Chain(lambda: 400),
                yes=Chain(lambda: 200),
            ),
        )
        assert_that(
            chain(arg=True),
            is_(equal_to(201)),
        )
        assert_that(
            chain(arg=False),
            is_(equal_to(401)),
        )
        assert_that(
            chain(arg=None),
            is_(equal_to(400)),
        )
        assert_that(
            chain(arg="yes"),
            is_(equal_to(200)),
        )

    def test_empty_switch(self):
        chain = Chain(
            switch(
                "arg",
                yes=Chain(lambda: 200),
            ),
        )
        assert_that(
            chain(arg=None),
            is_(equal_to(None)),
        )

    def test_try_chain(self):
        def function(exception):
            if exception is not None:
                raise exception()
            return 400

        chain = Chain(
            try_chain(
                Chain(function),
                catch=[
                    (ValueError, Chain(lambda: 501)),
                    (KeyError, Chain(lambda: 502)),
                ],
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

    def test_try_other(self):
        def function(exception):
            return 400

        chain = Chain(
            try_chain(
                Chain(function),
                otherwise=Chain(lambda: 200),
            ),
        )
        assert_that(
            chain(exception=None),
            is_(equal_to(200)),
        )
