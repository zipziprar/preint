import asyncio
import functools


def format_invoked_params(args, kwargs):
    return ', '.join([repr(arg) for arg in args] + [f"{key}={value!r}" for key, value in kwargs.items()])


class Reproducer:

    call_log = []

    @classmethod
    def do_log_call(cls, test_name, func_name, args, kwargs):
        arg_list = format_invoked_params(args, kwargs)
        cls.call_log.append(f"Test: {test_name}, Func: {func_name}({arg_list})")

    @classmethod
    def print_workflow(cls):
        for log in cls.call_log:
            print(log)

    @staticmethod
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Log the function call
            test_name = asyncio.current_task().get_name()
            Reproducer.do_log_call(test_name, func.__name__, args, kwargs)
            return await func(*args, **kwargs)
        return wrapper


def track_calls(cls):
    for key, value in cls.__dict__.items():
        if callable(value):
            setattr(cls, key, Reproducer.decorator(value))
    return cls


@track_calls
class VeryImportantTestHelpers:
    async def function_a(self, param_a, param_b):
        await asyncio.sleep(0.1)

    async def function_b(self, param_a, param_b):
        await asyncio.sleep(0.1)

    async def function_c(self, param_a, param_b):
        await asyncio.sleep(0.1)


class TestSamples:
    async def a_test(self):
        helpers = VeryImportantTestHelpers()
        for i in range(2):
            await helpers.function_a(i, "A")
            await helpers.function_b(i, "a")
            await helpers.function_a("A", i)

    async def b_test(self):
        helpers = VeryImportantTestHelpers()
        for i in range(2):
            await helpers.function_a(i, "B")
            await helpers.function_b(i, "b")


async def main():
    tasks = [
        asyncio.create_task(tests.a_test(), name='a_test'),
        asyncio.create_task(tests.b_test(), name='b_test')
    ]
    await asyncio.gather(*tasks)


tests = TestSamples()


if __name__ == "__main__":
    asyncio.run(main())
    Reproducer.print_workflow()
