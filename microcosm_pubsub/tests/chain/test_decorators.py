from hamcrest import (
    assert_that,
    calling,
    equal_to,
    is_,
    raises,
)

from microcosm_pubsub.chain.context_decorators import (
    get_from_context,
    save_to_context,
    save_to_context_by_func_name,
    temporarily_replace_context_keys,
)
from microcosm_pubsub.chain.decorators import binds, extracts
from microcosm_pubsub.chain.exceptions import ContextKeyNotFound


class TestDecorators:

    def test_get_from_context(self):
        context = dict(arg=200)
        wrapped = get_from_context(context, lambda arg: arg)
        assert_that(wrapped(), is_(200))

    def test_get_from_context_default_value(self):
        def function(a, b=10):
            return a + b

        context = dict()
        wrapped = get_from_context(context, function)
        assert_that(calling(wrapped), raises(ContextKeyNotFound))

        context = dict(a=190)
        wrapped = get_from_context(context, function)
        assert_that(wrapped(), is_(200))

        context = dict(a=190, b=11)
        wrapped = get_from_context(context, function)
        assert_that(wrapped(), is_(201))

    def test_save_to_context(self):
        context = dict()

        def dont_extract(number):
            return number

        @extracts("arg")
        def extract_arg(number):
            return number

        @extracts("arg1", "arg2")
        def extract_arg1_and_arg2(num1, num2):
            return num1, num2

        @extracts("args")
        def extract_args(num1, num2):
            return num1, num2

        @extracts("from_callable")
        class CallableExtractor:
            def __call__(self, number):
                return number

        class Extractor:
            def method(self, number):
                return number

            @classmethod
            def class_method(cls, number):
                return number

            @staticmethod
            def static_method(number):
                return number

        wrapped_method = extracts("from_method")(Extractor().method)
        wrapped_class_method = extracts("from_class_method")(Extractor().class_method)
        wrapped_static_method = extracts("from_static_method")(Extractor().static_method)

        save_to_context(context, dont_extract)(-1)
        save_to_context(context, extract_arg)(0)
        save_to_context(context, extract_arg1_and_arg2)(1, 2)
        save_to_context(context, extract_args)(1, 2)
        save_to_context(context, CallableExtractor())(3)
        save_to_context(context, wrapped_method)(4)
        save_to_context(context, wrapped_class_method)(5)
        save_to_context(context, wrapped_static_method)(6)

        assert_that(context, is_(equal_to(dict(
            arg=0,
            arg1=1,
            arg2=2,
            args=(1, 2),
            from_callable=3,
            from_method=4,
            from_class_method=5,
            from_static_method=6,
        ))))

    def test_overriding_extracts(self):
        context = dict()

        @extracts("arg")
        def extractarg(number):
            return number

        func = extracts("param")(extractarg)

        save_to_context(context, func)(10)

        assert_that(context, is_(equal_to(dict(param=10))))

    def test_save_to_context_by_func_name(self):
        context = dict()

        def extract_arg(number):
            return number

        def extract_args(num1, num2):
            return num1, num2

        save_to_context_by_func_name(context, extract_arg)(0)
        save_to_context_by_func_name(context, extract_args)(1, 2)
        assert_that(context, is_(equal_to(dict(arg=0, args=(1, 2)))))

    def test_get_from_context_multiple_args(self):
        context = dict(char2='b', char1='a')
        wrapped = get_from_context(context, lambda char1, char2: char1 + char2)
        assert_that(wrapped(), is_('ab'))

    def test_get_from_context_function_and_methods(self):
        context = dict(arg=123)

        def function(arg):
            return arg

        class example:
            class_number = 200

            def __init__(self):
                self.instance_number = 100

            def method(self, arg):
                return self.instance_number + arg

            @staticmethod
            def static_method(arg):
                return arg

            @classmethod
            def class_method(cls, arg):
                return cls.class_number + arg

        wrapped_function = get_from_context(context, function)
        assert_that(wrapped_function(), is_(123))

        wrapped_method = get_from_context(context, example().method)
        assert_that(wrapped_method(), is_(223))

        wrapped_static_method = get_from_context(context, example().static_method)
        assert_that(wrapped_static_method(), is_(123))

        wrapped_class_method = get_from_context(context, example().class_method)
        assert_that(wrapped_class_method(), is_(323))

        wrapped_static_method_ref = get_from_context(context, example.static_method)
        assert_that(wrapped_static_method_ref(), is_(123))

        wrapped_class_method_ref = get_from_context(context, example.class_method)
        assert_that(wrapped_class_method_ref(), is_(323))

    def test_temporarily_replace_context_keys(self):
        context = dict(arg=123)

        func = binds(arg="param")(lambda param: param)

        wrapped = get_from_context(context, func)
        wrapped = temporarily_replace_context_keys(context, wrapped)

        assert_that(wrapped(), is_(123))

    def test_temporarily_replace_context_keys_wrong_order(self):
        context = dict(arg=123)

        func = binds(arg="param")(lambda param: param)

        wrapped = temporarily_replace_context_keys(context, func)
        wrapped = get_from_context(context, wrapped)

        assert_that(calling(wrapped), raises(ContextKeyNotFound))

    def test_temporarily_replace_missing_keys(self):
        context = dict()

        func = binds(arg="param")(lambda param: param)

        wrapped = get_from_context(context, func)
        wrapped = temporarily_replace_context_keys(context, wrapped)

        assert_that(calling(wrapped), raises(KeyError))

    def test_multiple_decorators(self):
        @extracts("res_obj")
        class Extractor:
            def __call__(self, arg):
                return arg

        @extracts("res_func")
        def extractor(arg):
            return arg

        context = dict(arg=123)
        wrapped_object = save_to_context(context, get_from_context(context, Extractor()))
        wrapped_function = save_to_context(context, get_from_context(context, extractor))
        wrapped_object()
        wrapped_function()
        assert_that(context, is_(equal_to(dict(
            arg=123,
            res_obj=123,
            res_func=123,
        ))))
