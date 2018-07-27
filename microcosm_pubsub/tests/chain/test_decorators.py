from hamcrest import (
    assert_that,
    calling,
    equal_to,
    is_,
    raises,
)

from microcosm_pubsub.chain.decorators import binds, extracts
from microcosm_pubsub.chain.context_decorators import (
    get_from_context,
    save_to_context,
    save_to_context_by_func_name,
    temporarily_replace_context_keys,
)


class TestDecorators:

    def test_get_from_context(self):
        context = dict(arg=123)
        wrapped = get_from_context(context, lambda arg: arg)
        assert_that(wrapped(), is_(123))

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

        save_to_context(context, dont_extract)(-1)
        save_to_context(context, extract_arg)(0)
        save_to_context(context, extract_arg1_and_arg2)(1, 2)
        save_to_context(context, extract_args)(1, 2)

        assert_that(context, is_(equal_to(dict(arg=0, arg1=1, arg2=2, args=(1, 2)))))

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

    def test_temporarily_replace_context_keys_wring_order(self):
        context = dict(arg=123)

        func = binds(arg="param")(lambda param: param)

        wrapped = temporarily_replace_context_keys(context, func)
        wrapped = get_from_context(context, wrapped)

        assert_that(calling(wrapped), raises(TypeError))

    def test_temporarily_replace_missing_keys(self):
        context = dict()

        func = binds(arg="param")(lambda param: param)

        wrapped = get_from_context(context, func)
        wrapped = temporarily_replace_context_keys(context, wrapped)

        assert_that(calling(wrapped), raises(KeyError))
