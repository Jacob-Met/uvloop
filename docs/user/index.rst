User Guide
==========

This section of the documentation provides information about how to use
uvloop.


Installation
------------

`uvloop` is available from PyPI. It requires Python 3.9 or greater.

Use pip to install it.

.. code-block:: console

    $ pip install uvloop


Using uvloop
------------

To make asyncio use the event loop provided by `uvloop`, you install the
`uvloop` event loop policy:

.. code-block:: python

    import asyncio
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


Alternatively, you can create an instance of the loop manually, using:

.. code-block:: python

    import asyncio
    import uvloop
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)


Dual-stack TCP connections (Happy Eyeballs)
------------------------------------------

To avoid long connection delays when one IP address family is unreachable,
pass ``happy_eyeballs_delay`` to ``asyncio.open_connection()`` (or
``loop.create_connection()``). uvloop staggers IPv6 and IPv4 TCP
connection attempts, using the first successful connection and closing
all losing sockets without constructing additional application protocols:

.. code-block:: python

    import asyncio
    import uvloop

    async def main():
        reader, writer = await asyncio.open_connection(
            "example.com", 80,
            happy_eyeballs_delay=0.25, interleave=1)
        try:
            writer.write(
                b"HEAD / HTTP/1.1\r\nHost: example.com\r\n"
                b"Connection: close\r\n\r\n")
            await writer.drain()
            print((await reader.readline()).decode().strip())
        finally:
            writer.close()
            await writer.wait_closed()

    uvloop.run(main())

``interleave=1`` alternates IPv4/IPv6 address candidates while preserving
resolver order within each family. It can also be used without
``happy_eyeballs_delay`` to retain serial attempts. Apps without
these options preserve the existing connection order. On Python 3.11+,
``all_errors=True`` collects failed attempts into an
``ExceptionGroup``.
