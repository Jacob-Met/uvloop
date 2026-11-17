"""Measure socket waiter overhead using real, nonblocking UNIX sockets.

Run this same script under the unmodified uvloop revision and under the
candidate revision, alternating processes to reduce drift:

    python examples/bench/socket_futures.py --rounds 5 --iters 7000 \
        --fanout 512 --cycles 40

No external traffic, third-party benchmark packages, or mocked file
descriptors are involved.  The cancellation scenario verifies that a
cancelled read never consumes the next read's data.
"""

import argparse
import asyncio
import gc
import json
import platform
import resource
import socket
import statistics
import time

import uvloop


async def hot_read(count):
    writer, reader = socket.socketpair()
    writer.setblocking(False)
    reader.setblocking(False)
    loop = asyncio.get_running_loop()
    try:
        start = time.perf_counter_ns()
        for _ in range(count):
            assert writer.send(b'x') == 1
            assert await loop.sock_recv(reader, 1) == b'x'
        return time.perf_counter_ns() - start, count
    finally:
        writer.close()
        reader.close()


async def hot_duplex(count):
    writer, reader = socket.socketpair()
    writer.setblocking(False)
    reader.setblocking(False)
    loop = asyncio.get_running_loop()
    try:
        start = time.perf_counter_ns()
        for _ in range(count):
            await loop.sock_sendall(writer, b'x')
            assert await loop.sock_recv(reader, 1) == b'x'
        return time.perf_counter_ns() - start, count
    finally:
        writer.close()
        reader.close()


async def fanout(concurrency, cycles):
    pairs = [socket.socketpair() for _ in range(concurrency)]
    for writer, reader in pairs:
        writer.setblocking(False)
        reader.setblocking(False)
    loop = asyncio.get_running_loop()
    try:
        start = time.perf_counter_ns()
        for _ in range(cycles):
            tasks = [
                loop.create_task(loop.sock_recv(reader, 1))
                for _, reader in pairs
            ]
            await asyncio.sleep(0)
            for writer, _ in pairs:
                assert writer.send(b'z') == 1
            results = await asyncio.gather(*tasks)
            assert all(result == b'z' for result in results)
        return time.perf_counter_ns() - start, concurrency * cycles
    finally:
        for writer, reader in pairs:
            writer.close()
            reader.close()


async def cancel(concurrency, cycles):
    pairs = [socket.socketpair() for _ in range(concurrency)]
    for writer, reader in pairs:
        writer.setblocking(False)
        reader.setblocking(False)
    loop = asyncio.get_running_loop()
    try:
        start = time.perf_counter_ns()
        for _ in range(cycles):
            tasks = [
                loop.create_task(loop.sock_recv(reader, 1))
                for _, reader in pairs
            ]
            await asyncio.sleep(0)
            for task in tasks:
                task.cancel()
            results = await asyncio.gather(*tasks, return_exceptions=True)
            assert all(
                isinstance(result, asyncio.CancelledError)
                for result in results
            )

            # The next operation must still own the descriptor's reader.
            for writer, _ in pairs:
                assert writer.send(b'z') == 1
            results = await asyncio.gather(*(
                loop.sock_recv(reader, 1) for _, reader in pairs
            ))
            assert all(result == b'z' for result in results)
        return time.perf_counter_ns() - start, concurrency * cycles
    finally:
        for writer, reader in pairs:
            writer.close()
            reader.close()


async def run(args):
    scenarios = (
        ('hot_read', lambda: hot_read(args.iters)),
        ('hot_duplex', lambda: hot_duplex(args.iters)),
        ('fanout', lambda: fanout(args.fanout, args.cycles)),
        ('cancel', lambda: cancel(args.fanout, args.cycles)),
    )
    readings = {}
    for name, scenario in scenarios:
        samples = []
        for _ in range(args.rounds):
            gc.collect()
            elapsed_ns, operations = await scenario()
            samples.append(elapsed_ns / operations)
        readings[name] = {
            'ns_per_op_median': round(statistics.median(samples), 1),
            'ns_per_op_samples': [round(sample, 1) for sample in samples],
        }

    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    rss_bytes = rss if platform.system() == 'Darwin' else rss * 1024
    print(json.dumps({
        'python': platform.python_version(),
        'uvloop': uvloop.__version__,
        'params': vars(args),
        'rss_bytes_max': rss_bytes,
        'scenarios': readings,
    }, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rounds', type=int, default=3)
    parser.add_argument('--iters', type=int, default=5000)
    parser.add_argument('--fanout', type=int, default=128)
    parser.add_argument('--cycles', type=int, default=25)
    args = parser.parse_args()

    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run(args))
    finally:
        loop.close()
        asyncio.set_event_loop(None)


if __name__ == '__main__':
    main()
