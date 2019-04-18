"""
(Potentially blocking IO-filled) workers grabbing things off of a base queue,
filled from a(n also-blocking) daemon thread,
putting results onto another queue.
"""
import asyncio
import time
from queue import Queue
from threading import Thread


N_WORKERS = 2
GENERATION_SLEEP = 1
PROCESSING_SLEEP = 2


def queue_filler(q: Queue, n: int = 10):
    # Add things to a queue in a daemon thread
    for i in range(n):
        time.sleep(GENERATION_SLEEP)
        q.put(i)
    
    # Uses None as a signal to say I'm done.
    for _ in range(N_WORKERS):
        q.put(None)


def do_work(in_q: Queue, out_q: asyncio.Queue):
    while True:
        i = in_q.get()
        if i is None:
            in_q.task_done()
            return
        time.sleep(PROCESSING_SLEEP)
        result = i ** 2
        in_q.task_done()
        out_q.put_nowait(result)


async def main():
    # Start the workload creating thread
    work_queue = Queue()
    creator = Thread(target=queue_filler, args=(work_queue,))
    creator.daemon = True
    creator.start()

    # Start workers grabbing things off of the thread
    result_queue = asyncio.Queue()
    loop = asyncio.get_event_loop()
    workers = [
        loop.run_in_executor(None, do_work, work_queue, result_queue)
        for _ in range(N_WORKERS)
    ]

    await asyncio.gather(*workers)
    while not result_queue.empty():
        print(await result_queue.get())


if __name__ == '__main__':
    start = time.time()
    asyncio.run(main())
    end = time.time()
    print(f"Took {end - start:.02f} seconds")
