import asyncio
from contextlib import suppress
from typing import Iterable, Awaitable, TypeVar


SingleReturnType = TypeVar("SingleReturnType")


async def run_in_parallel(
    coroutine_generator: Iterable[Awaitable[SingleReturnType]], max_parallel: int
) -> list[SingleReturnType]:
    """
    Run tasks from this generator in parallel, with some fixed max parallelism.
    """
    tasks: set[asyncio.Future[SingleReturnType]] = set()
    results: list[SingleReturnType] = []
    for routine in coroutine_generator:
        if len(tasks) >= max_parallel:
            done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            finished_task = done.pop()
            exception = finished_task.exception()
            if exception is not None:
                # Cancel all the other tasks, then bubble up the exception
                for task in tasks:
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
                raise exception
            results.append(finished_task.result())
        tasks.add(asyncio.create_task(routine))
    final_results = await asyncio.gather(*tasks)
    return results + list(final_results)
