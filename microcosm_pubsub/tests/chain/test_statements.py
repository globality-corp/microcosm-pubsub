from hamcrest import (
    assert_that,
    calling,
    equal_to,
    is_,
    raises,
)

from microcosm_pubsub.chain import Chain
from microcosm_pubsub.chain.statements import (
    assign,
    extract,
    for_each,
    when,
    switch,
    try_chain,
)


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

    def test_extractwith_property(self):
        chain = Chain(
            extract("param", "arg", "data"),
        )
        assert_that(
            chain(arg=dict(data=200)),
            is_(equal_to(200)),
        )

    def test_extractwith_property_simplified(self):
        chain = Chain(
            extract("param", "arg.data"),
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

    def test_extractwith_attribute_simplified(self):
        chain = Chain(
            extract("param", "arg.__class__"),
        )
        assert_that(
            chain(arg=dict()),
            is_(equal_to(dict)),
        )

    def test_assign_property(self):
        chain = Chain(
            assign("arg.data").to("param"),
        )
        assert_that(
            chain(arg=dict(data=200)),
            is_(equal_to(200)),
        )

    def test_assign_attribute(self):
        chain = Chain(
            assign("arg.__class__").to("param"),
        )
        assert_that(
            chain(arg=dict()),
            is_(equal_to(dict)),
        )

    def test_when(self):
        chain = Chain(
            when("arg").then(
                Chain(lambda: 200),
            ).otherwise(
                Chain(lambda: 400),
            ),
        )
        assert_that(
            chain(arg=True),
            is_(equal_to(200)),
        )
        assert_that(
            chain(arg=False),
            is_(equal_to(400)),
        )

    def test_when_simplified(self):
        chain = Chain(
            when("arg").then(
                lambda: 200,
            ).otherwise(
                lambda: 400,
            ),
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
            switch("arg").case(
                True, Chain(lambda: 200),
            ).case(
                False, Chain(lambda: 400),
            ).otherwise(
                Chain(lambda: 500),
            ),
        )
        assert_that(
            chain(arg=True),
            is_(equal_to(200)),
        )
        assert_that(
            chain(arg=False),
            is_(equal_to(400)),
        )
        assert_that(
            chain(arg=None),
            is_(equal_to(500)),
        )

    def test_switch_simplified(self):
        chain = Chain(
            switch("arg").case(True).then(
                lambda: 200,
            ).case(False).then(
                lambda: 400,
            ).otherwise(
                lambda: 500,
            ),
        )
        assert_that(
            chain(arg=True),
            is_(equal_to(200)),
        )
        assert_that(
            chain(arg=False),
            is_(equal_to(400)),
        )
        assert_that(
            chain(arg=None),
            is_(equal_to(500)),
        )

    def test_for_each(self):
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

    def test_for_each_simplified(self):
        chain = Chain(
            for_each("item").in_("items").do(
                lambda item: item.upper(),
            ),
            lambda item_list: list(reversed(item_list)),
        )

        assert_that(
            chain(items=["a", "b", "c"]),
            is_(equal_to(["C", "B", "A"])),
        )

    def test_empty_switch(self):
        chain = Chain(
            switch("arg"),
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
            ).catch(
                ValueError, Chain(lambda: 501),
            ).catch(
                KeyError, Chain(lambda: 502),
            )
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

    def test_try_chain_simplified(self):
        def function(exception):
            if exception is not None:
                raise exception()
            return 400

        chain = Chain(
            try_chain(
                function,
            ).catch(ValueError).then(
                lambda: 501,
            ).catch(KeyError).then(
                lambda: 502,
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
            ).otherwise(
                Chain(lambda: 200),
            ),
        )
        assert_that(
            chain(exception=None),
            is_(equal_to(200)),
        )

    def test_try_other_simplified(self):
        def function(exception):
            return 400

        chain = Chain(
            try_chain(
                function,
            ).otherwise(
                lambda: 200,
            ),
        )
        assert_that(
            chain(exception=None),
            is_(equal_to(200)),
        )
