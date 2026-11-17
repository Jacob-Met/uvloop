"""RFC 8305 connect racing and IPv4/IPv6 interleaving regressions."""

import asyncio
import os
import socket
import ssl
import sys
import time
import unittest
from unittest import mock

import uvloop
from uvloop import _happy_eyeballs


class TestHappyEyeballs(unittest.TestCase):
    def setUp(self):
        self.loop = uvloop.new_event_loop()

    def tearDown(self):
        self.loop.close()

    def test_connect_with_staggered_racing(self):
        created = []

        done = asyncio.Event()

        async def handler(reader, writer):
            try:
                await reader.read(1)
            finally:
                writer.close()
                await writer.wait_closed()
                done.set()

        async def run():
            server = await asyncio.start_server(handler, '127.0.0.1', 0)
            try:
                port = server.sockets[0].getsockname()[1]

                def factory():
                    proto = asyncio.Protocol()
                    created.append(proto)
                    return proto

                transport, protocol = await self.loop.create_connection(
                    factory, '127.0.0.1', port,
                    happy_eyeballs_delay=0.01, interleave=1)
                try:
                    self.assertIs(protocol, created[0])
                    self.assertEqual(len(created), 1)
                    self.assertEqual(
                        transport.get_extra_info('peername')[1], port)
                    transport.write(b'x')
                    await asyncio.wait_for(done.wait(), 1.0)
                finally:
                    transport.close()
            finally:
                server.close()
                await server.wait_closed()

        self.loop.run_until_complete(run())

    def test_interleave_without_racing(self):
        async def handler(reader, writer):
            writer.close()
            await writer.wait_closed()

        async def run():
            server = await asyncio.start_server(handler, '127.0.0.1', 0)
            try:
                port = server.sockets[0].getsockname()[1]
                transport, _ = await self.loop.create_connection(
                    asyncio.Protocol, '127.0.0.1', port, interleave=1)
                transport.close()
            finally:
                server.close()
                await server.wait_closed()

        self.loop.run_until_complete(run())

    def test_preconnected_socket_with_race_options(self):
        async def handler(reader, writer):
            writer.close()
            await writer.wait_closed()

        async def run():
            server = await asyncio.start_server(handler, '127.0.0.1', 0)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                await self.loop.sock_connect(
                    sock, server.sockets[0].getsockname())
                transport, _ = await self.loop.create_connection(
                    asyncio.Protocol, sock=sock,
                    happy_eyeballs_delay=0.01, interleave=1)
                transport.close()
            finally:
                sock.close()
                server.close()
                await server.wait_closed()

        self.loop.run_until_complete(run())

    def test_local_addr_with_race(self):
        async def handler(reader, writer):
            writer.close()
            await writer.wait_closed()

        async def run():
            server = await asyncio.start_server(handler, '127.0.0.1', 0)
            try:
                port = server.sockets[0].getsockname()[1]
                transport, _ = await self.loop.create_connection(
                    asyncio.Protocol, '127.0.0.1', port,
                    local_addr=('127.0.0.1', 0),
                    happy_eyeballs_delay=0.01)
                self.assertEqual(
                    transport.get_extra_info('sockname')[0], '127.0.0.1')
                transport.close()
            finally:
                server.close()
                await server.wait_closed()

        self.loop.run_until_complete(run())

    def test_non_ssl_options_preserve_ssl_validation(self):
        with self.assertRaisesRegex(ValueError, 'only meaningful with ssl'):
            self.loop.run_until_complete(self.loop.create_connection(
                asyncio.Protocol, '127.0.0.1', 1,
                happy_eyeballs_delay=0.01, server_hostname='wrong'))

    def test_tls_handshake_after_race_with_one_protocol_factory(self):
        certs = os.path.join(os.path.dirname(__file__), 'certs')
        server_ssl = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_ssl.load_cert_chain(
            os.path.join(certs, 'ssl_cert.pem'),
            os.path.join(certs, 'ssl_key.pem'))
        client_ssl = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        client_ssl.check_hostname = False
        client_ssl.verify_mode = ssl.CERT_NONE
        created = []
        done = self.loop.create_future()

        class Protocol(asyncio.Protocol):
            def connection_made(self, transport):
                self.transport = transport
                transport.write(b'ping')

            def data_received(self, data):
                if not done.done():
                    done.set_result(data)

            def connection_lost(self, error):
                if error and not done.done():
                    done.set_exception(error)

        def factory():
            instance = Protocol()
            created.append(instance)
            return instance

        async def handler(reader, writer):
            try:
                data = await reader.readexactly(4)
                writer.write(data.upper())
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        async def run():
            server = await asyncio.start_server(
                handler, '127.0.0.1', 0, ssl=server_ssl)
            try:
                port = server.sockets[0].getsockname()[1]
                transport, protocol = await self.loop.create_connection(
                    factory, '127.0.0.1', port, ssl=client_ssl,
                    server_hostname='localhost',
                    happy_eyeballs_delay=0.01, interleave=1)
                try:
                    self.assertIs(created[0], protocol)
                    self.assertEqual(len(created), 1)
                    self.assertEqual(
                        await asyncio.wait_for(done, 1.0), b'PING')
                finally:
                    transport.close()
                    await asyncio.sleep(0.05)
            finally:
                server.close()
                await server.wait_closed()

        self.loop.run_until_complete(run())

    @unittest.skipIf(
        sys.version_info < (3, 11), 'ExceptionGroup requires Python 3.11')
    def test_all_errors_are_grouped(self):
        async def run():
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                port = sock.getsockname()[1]
            with self.assertRaises(ExceptionGroup) as exc:
                await self.loop.create_connection(
                    asyncio.Protocol, '127.0.0.1', port,
                    all_errors=True, happy_eyeballs_delay=0.01)
            self.assertEqual(len(exc.exception.exceptions), 1)
            self.assertIsInstance(exc.exception.exceptions[0], OSError)

        self.loop.run_until_complete(run())

    def test_racing_requires_real_address(self):
        with self.assertRaises(ValueError):
            self.loop.run_until_complete(self.loop.create_connection(
                asyncio.Protocol, happy_eyeballs_delay=0.01))


class _ControlledLoop:
    """Real event-loop task scheduling with controlled connection I/O."""

    def __init__(self, loop, remote_infos, delays, local_infos=None):
        self.loop = loop
        self.remote_infos = remote_infos
        self.local_infos = local_infos
        self.delays = delays
        self.attempts = []

    def create_task(self, coroutine):
        return self.loop.create_task(coroutine)

    def time(self):
        return self.loop.time()

    async def getaddrinfo(self, host, port, **kwargs):
        if host == 'local':
            return self.local_infos
        return self.remote_infos

    async def sock_connect(self, sock, address):
        self.attempts.append((sock, address, time.monotonic()))
        delay, error = self.delays[address[0]]
        await asyncio.sleep(delay)
        if error:
            raise OSError(error)


class TestHappyEyeballsRacing(unittest.TestCase):
    def setUp(self):
        self.loop = uvloop.new_event_loop()
        self.infos = [
            (socket.AF_INET6, socket.SOCK_STREAM, 6, '', ('::1', 80, 0, 0)),
            (socket.AF_INET6, socket.SOCK_STREAM, 6, '', ('::2', 80, 0, 0)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('127.0.0.1', 80)),
        ]

    def tearDown(self):
        self.loop.close()

    def test_rfc8305_family_interleaving(self):
        values = self.infos
        self.assertEqual(
            _happy_eyeballs._interleave_addrinfos(values, 1),
            [values[0], values[2], values[1]])
        self.assertEqual(
            _happy_eyeballs._interleave_addrinfos(values, 2), values)
        self.assertEqual(
            _happy_eyeballs._interleave_addrinfos([], 1), [])

    def test_early_ipv4_winner_cancels_ipv6_without_fd_leak(self):
        controlled = _ControlledLoop(
            self.loop, self.infos,
            {'::1': (0.3, None), '::2': (0.3, None),
             '127.0.0.1': (0.002, None)})

        async def run():
            start = time.monotonic()
            winner = await _happy_eyeballs._connect_socket(
                controlled, 'remote', 80, 0, 0, 0, None, 0.01, None)
            duration = time.monotonic() - start
            try:
                self.assertLess(duration, 0.12)
                self.assertEqual(winner.family, socket.AF_INET)
                self.assertEqual(
                    [a[1][0] for a in controlled.attempts],
                    ['::1', '127.0.0.1'])
                self.assertEqual(
                    [a[0].fileno() == -1 for a in controlled.attempts],
                    [True, False])
            finally:
                winner.close()

        self.loop.run_until_complete(run())

    def test_multiple_simultaneous_winners_close_losing_sockets(self):
        controlled = _ControlledLoop(
            self.loop, self.infos,
            {'::1': (0, None), '::2': (0, None),
             '127.0.0.1': (0, None)})
        all_started = asyncio.Event()

        async def simultaneous_connect(sock, addr):
            controlled.attempts.append((sock, addr, time.monotonic()))
            if len(controlled.attempts) == len(self.infos):
                all_started.set()
            await all_started.wait()

        controlled.sock_connect = simultaneous_connect

        async def run():
            winner = await _happy_eyeballs._connect_socket(
                controlled, 'remote', 80, 0, 0, 0, None, 0, 1)
            try:
                self.assertGreaterEqual(len(controlled.attempts), 2)
                live = [
                    sock for sock, _, _ in controlled.attempts
                    if sock.fileno() != -1]
                self.assertEqual(live, [winner])
            finally:
                winner.close()
            self.assertTrue(all(
                sock.fileno() == -1
                for sock, _, _ in controlled.attempts))

        self.loop.run_until_complete(run())

    def test_exhausted_candidates_propagate_all_errors(self):
        controlled = _ControlledLoop(
            self.loop, self.infos,
            {'::1': (0, 'ipv6-first'), '::2': (0, 'ipv6-second'),
             '127.0.0.1': (0, 'ipv4')})

        async def run():
            if sys.version_info >= (3, 11):
                with self.assertRaises(ExceptionGroup) as exc:
                    await _happy_eyeballs._connect_socket(
                        controlled, 'remote', 80, 0, 0, 0,
                        None, 0.01, 1, all_errors=True)
                self.assertEqual(len(exc.exception.exceptions), 3)
            else:
                with self.assertRaises(NotImplementedError):
                    await _happy_eyeballs._connect_socket(
                        controlled, 'remote', 80, 0, 0, 0,
                        None, 0.01, 1, all_errors=True)
            self.assertTrue(all(
                sock.fileno() == -1
                for sock, _, _ in controlled.attempts))

        self.loop.run_until_complete(run())

    def test_cancellation_closes_all_racing_descriptors(self):
        controlled = _ControlledLoop(
            self.loop, self.infos,
            {'::1': (0.5, None), '::2': (0.5, None),
             '127.0.0.1': (0.5, None)})

        async def run():
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(
                    _happy_eyeballs._connect_socket(
                        controlled, 'remote', 80, 0, 0, 0,
                        None, 0.005, 1), timeout=0.03)
            self.assertGreaterEqual(len(controlled.attempts), 2)
            self.assertTrue(all(
                sock.fileno() == -1
                for sock, _, _ in controlled.attempts))

        self.loop.run_until_complete(run())

    def test_sequential_interleave_respects_family_order(self):
        controlled = _ControlledLoop(
            self.loop, self.infos,
            {'::1': (0.001, 'first failed'), '::2': (0.001, None),
             '127.0.0.1': (0.001, None)})

        async def run():
            winner = await _happy_eyeballs._connect_socket(
                controlled, 'remote', 80, 0, 0, 0, None, None, 1)
            try:
                self.assertEqual(winner.family, socket.AF_INET)
                self.assertEqual(
                    [a[1][0] for a in controlled.attempts],
                    ['::1', '127.0.0.1'])
            finally:
                winner.close()

        self.loop.run_until_complete(run())

    def test_no_local_family_leaks_sockets(self):
        controlled = _ControlledLoop(
            self.loop, [self.infos[2]],
            {'127.0.0.1': (0.001, None)},
            local_infos=[self.infos[0]])

        real_socket = socket.socket
        created = []

        def track_socket(*args, **kwargs):
            sock = real_socket(*args, **kwargs)
            created.append(sock)
            return sock

        async def run():
            with mock.patch.object(
                    _happy_eyeballs.socket, 'socket',
                    side_effect=track_socket):
                with self.assertRaisesRegex(
                        OSError, 'no matching local address'):
                    await _happy_eyeballs._connect_socket(
                        controlled, 'remote', 80, 0, 0, 0,
                        ('local', 0), 0.01, 1)
            self.assertEqual(len(created), 1)
            self.assertEqual(created[0].fileno(), -1)

        self.loop.run_until_complete(run())
