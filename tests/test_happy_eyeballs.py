import asyncio
import socket
import ssl
from pathlib import Path
import sys
import time
import unittest

import uvloop
from uvloop import loop as native


class TestHappyEyeballs(unittest.TestCase):
    def setUp(self):
        self.loop = uvloop.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()
        asyncio.set_event_loop(None)

    def test_numeric_addrinfos_are_not_resolved_twice(self):
        class CountingLoop(uvloop.Loop):
            calls = 0

            async def getaddrinfo(self, *args, **kwargs):
                self.calls += 1
                return await super().getaddrinfo(*args, **kwargs)

        self.loop.close()
        self.loop = CountingLoop()
        asyncio.set_event_loop(self.loop)
        with socket.socket() as server:
            server.bind(('127.0.0.1', 0))
            server.listen(1)
            addr = server.getsockname()
            transport, _ = self.loop.run_until_complete(
                self.loop.create_connection(
                    asyncio.Protocol, *addr,
                    happy_eyeballs_delay=0.02))
            self.assertEqual(self.loop.calls, 1)
            accepted, _ = server.accept()
            accepted.close()
            transport.close()
            self.loop.run_until_complete(asyncio.sleep(0))

    def test_family_interleave_preserves_order(self):
        v6 = socket.AF_INET6
        v4 = socket.AF_INET
        infos = [(v6, 'a'), (v6, 'b'), (v6, 'c'),
                 (v4, 'd'), (v4, 'e')]
        interleave = native._interleave_connection_addrinfos
        self.assertEqual([x[1] for x in interleave(infos, 1)],
                         ['a', 'd', 'b', 'e', 'c'])
        self.assertEqual([x[1] for x in interleave(infos, 2)],
                         ['a', 'b', 'd', 'c', 'e'])

    def test_real_connected_socket_and_local_bind(self):
        with socket.socket() as server:
            server.bind(('127.0.0.1', 0))
            server.listen(3)
            addr = server.getsockname()
            made = []

            class Protocol(asyncio.Protocol):
                def connection_made(self, transport):
                    made.append(transport)

            for delay, interleave in ((0.02, None), (None, 1),
                                      (None, None)):
                transport, proto = self.loop.run_until_complete(
                    self.loop.create_connection(
                        Protocol, *addr, family=socket.AF_INET,
                        happy_eyeballs_delay=delay,
                        interleave=interleave,
                        local_addr=('127.0.0.1', 0)))
                self.assertIs(transport, made[-1])
                self.assertEqual(transport.get_extra_info('sockname')[0],
                                 '127.0.0.1')
                server.settimeout(1)
                accepted, _ = server.accept()
                accepted.close()
                transport.close()
                self.loop.run_until_complete(asyncio.sleep(0))

    def test_ssl_one_protocol_and_no_losing_handshakes(self):
        server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        certs = Path(__file__).parent / 'certs'
        server_ctx.load_cert_chain(certs / 'ssl_cert.pem',
                                   certs / 'ssl_key.pem')
        client_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        client_ctx.check_hostname = False
        client_ctx.verify_mode = ssl.CERT_NONE
        factory_calls = []

        class Echo(asyncio.Protocol):
            def connection_made(self, transport):
                self.transport = transport

            def data_received(self, data):
                self.transport.write(data)

        class Client(asyncio.Protocol):
            def connection_made(self, transport):
                self.transport = transport

            def data_received(self, data):
                if not received.done():
                    received.set_result(data)

        def factory():
            factory_calls.append(True)
            return Client()

        async def run():
            nonlocal received
            received = self.loop.create_future()
            server = await self.loop.create_server(
                Echo, '127.0.0.1', 0, ssl=server_ctx)
            address = server.sockets[0].getsockname()
            try:
                transport, protocol = await self.loop.create_connection(
                    factory, *address, ssl=client_ctx,
                    server_hostname='localhost',
                    happy_eyeballs_delay=0.01)
                self.assertEqual(len(factory_calls), 1)
                transport.write(b'ping')
                self.assertEqual(await asyncio.wait_for(received, 2), b'ping')
                transport.close()
                await asyncio.sleep(0.02)
            finally:
                server.close()
                await server.wait_closed()

        received = None
        self.loop.run_until_complete(run())

    def test_multiple_racing_errors_in_exception_group(self):
        loop = self.loop

        class FailingLoop:
            create_task = loop.create_task

            async def sock_connect(self, sock, addr):
                raise OSError('failed on ' + addr[0])

        def info(name):
            return (socket.AF_INET, socket.SOCK_STREAM, 0, '',
                    (name, 8765))
        with self.assertRaises(ExceptionGroup) as ctx:
            loop.run_until_complete(native._race_connection_sockets(
                FailingLoop(), [info('one'), info('two')],
                None, 0.001, 0, True))
        self.assertEqual(len(ctx.exception.exceptions), 2)

    def test_connection_failure_exception_groups(self):
        with socket.socket() as server:
            server.bind(('127.0.0.1', 0))
            port = server.getsockname()[1]
        for delay in (None, 0.01):
            if sys.version_info >= (3, 11):
                with self.assertRaises(ExceptionGroup) as ctx:
                    self.loop.run_until_complete(
                        self.loop.create_connection(
                            asyncio.Protocol, '127.0.0.1', port,
                            happy_eyeballs_delay=delay, all_errors=True))
                self.assertTrue(ctx.exception.exceptions)
            with self.assertRaises(ConnectionRefusedError):
                self.loop.run_until_complete(
                    self.loop.create_connection(
                        asyncio.Protocol, '127.0.0.1', port,
                        happy_eyeballs_delay=delay))

    def test_staggered_slow_candidate_is_cancelled_and_closed(self):
        loop = self.loop
        seen = {}

        class MockConnectLoop:
            create_task = loop.create_task

            async def sock_connect(self, sock, addr):
                seen[addr[0]] = sock
                if addr[0] == 'slow':
                    try:
                        await asyncio.Event().wait()
                    finally:
                        seen['cancelled'] = True
                else:
                    await asyncio.sleep(0)

        def info(name):
            return (socket.AF_INET, socket.SOCK_STREAM, 0, '',
                    (name, 8765))

        started = time.monotonic()
        winner = loop.run_until_complete(native._race_connection_sockets(
            MockConnectLoop(), [info('slow'), info('fast')],
            None, 0.02, 0, False))
        try:
            self.assertLess(time.monotonic() - started, 0.5)
            self.assertTrue(seen['cancelled'])
            self.assertEqual(seen['slow'].fileno(), -1)
            self.assertIs(winner, seen['fast'])
        finally:
            winner.close()
