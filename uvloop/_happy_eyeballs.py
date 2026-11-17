"""RFC 8305 address selection for uvloop's native TCP connector.

Build sockets before constructing the application protocol. Losing or
cancelled attempts cannot run application callbacks or acquire SSL state.
Only the connected winner is transferred to the native TCP transport.
"""

import asyncio
import builtins
import collections
import itertools
import socket
from typing import Any, Iterable, NoReturn, Optional

AddressInfo = tuple[int, int, int, str, tuple[Any, ...]]


def _interleave_addrinfos(
    addrinfos: list[AddressInfo], first_address_family_count: int,
) -> list[AddressInfo]:
    """Keep resolver order within each family while alternating families."""
    families: dict[int, list[AddressInfo]] = collections.OrderedDict()
    for addrinfo in addrinfos:
        families.setdefault(addrinfo[0], []).append(addrinfo)
    groups = list(families.values())
    if not groups:
        return []
    first = groups[0][:first_address_family_count - 1]
    del groups[0][:first_address_family_count - 1]
    return first + [
        item
        for group in itertools.zip_longest(*groups)
        for item in group
        if item is not None
    ]


def _bind_local(
    sock: socket.socket, address_family: int,
    local_addrinfos: Optional[list[AddressInfo]],
) -> None:
    if not local_addrinfos:
        return
    errors = []
    matched = False
    for family, _, _, _, local_address in local_addrinfos:
        if family != address_family:
            continue
        matched = True
        try:
            sock.bind(local_address)
            return
        except OSError as exc:
            errors.append(exc)
    if errors:
        if len(errors) == 1:
            raise errors[0]
        raise OSError('Multiple local address errors: {}'.format(
            ', '.join(str(exc) for exc in errors)))
    if not matched:
        raise OSError('no matching local address for resolved remote family')


async def _connect_one(
    loop: Any, address_info: AddressInfo,
    local_addrinfos: Optional[list[AddressInfo]],
) -> socket.socket:
    family, socktype, proto, _, remote_address = address_info
    sock = socket.socket(family, socktype, proto)
    try:
        sock.setblocking(False)
        _bind_local(sock, family, local_addrinfos)
        await loop.sock_connect(sock, remote_address)
        return sock
    except BaseException:
        # Cancellation must discard a losing socket and its native writer.
        sock.close()
        raise


def _raise_connect_errors(
    errors: Iterable[BaseException], all_errors: bool,
) -> NoReturn:
    exceptions: list[OSError] = [
        exc for exc in errors if isinstance(exc, OSError)
    ]
    if not exceptions:
        raise OSError('Connection attempts did not produce a result')
    if all_errors:
        group_type = getattr(builtins, 'ExceptionGroup', None)
        if group_type is None:
            raise NotImplementedError(
                'all_errors requires ExceptionGroup (Python 3.11+)')
        raise group_type('create_connection failed', exceptions)
    if len(exceptions) == 1:
        raise exceptions[0]
    if all(str(exc) == str(exceptions[0]) for exc in exceptions):
        raise exceptions[0]
    raise OSError('Multiple exceptions: {}'.format(
        ', '.join(str(exc) for exc in exceptions)))


async def _connect_socket(
    loop: Any, host: Any, port: Any, family: int, proto: int, flags: int,
    local_addr: Any, happy_eyeballs_delay: Optional[float],
    interleave: Optional[int], all_errors: bool = False,
) -> socket.socket:
    """Return exactly one connected raw socket, closing all failed attempts."""
    remote_infos = await loop.getaddrinfo(
        host, port, family=family, type=socket.SOCK_STREAM,
        proto=proto, flags=flags)
    if not remote_infos:
        raise OSError('getaddrinfo() returned empty list')
    local_infos: Optional[list[AddressInfo]] = None
    if local_addr is not None:
        if not isinstance(local_addr, (tuple, list)) or len(local_addr) != 2:
            raise ValueError('local_addr must be a tuple of host and port')
        local_infos = await loop.getaddrinfo(
            *local_addr, family=family, type=socket.SOCK_STREAM,
            proto=proto, flags=flags)
        if not local_infos:
            raise OSError('getaddrinfo() returned empty list for local_addr')

    if happy_eyeballs_delay is not None and interleave is None:
        interleave = 1
    if interleave:
        remote_infos = _interleave_addrinfos(remote_infos, interleave)

    if happy_eyeballs_delay is None:
        exceptions: list[OSError] = []
        for info in remote_infos:
            try:
                return await _connect_one(loop, info, local_infos)
            except OSError as exc:
                exceptions.append(exc)
        _raise_connect_errors(exceptions, all_errors)

    # Unlike asyncio.staggered.staggered_race, await *all* cancelled loser
    # tasks before leaving. The underlying socket is owned by its task until
    # that task finishes its cancellation handler and closes the descriptor.
    tasks: list[asyncio.Task[socket.socket]] = []
    exceptions = []
    winner: Optional[socket.socket] = None
    succeeded = False
    index = 0
    next_launch = 0.0
    try:
        while index < len(remote_infos) or tasks:
            if not tasks and index < len(remote_infos):
                tasks.append(loop.create_task(
                    _connect_one(loop, remote_infos[index], local_infos)))
                index += 1
                next_launch = loop.time() + happy_eyeballs_delay

            timeout = None
            if index < len(remote_infos):
                timeout = max(0, next_launch - loop.time())
            done, _ = await asyncio.wait(
                tasks, timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED)
            if done:
                # Preserve deterministic resolver order for simultaneous
                # successes; loser sockets are closed in the finally block.
                for task in list(tasks):
                    if task not in done:
                        continue
                    tasks.remove(task)
                    try:
                        result = task.result()
                    except OSError as exc:
                        exceptions.append(exc)
                    else:
                        if winner is None:
                            winner = result
                        else:
                            result.close()
                if winner is not None:
                    succeeded = True
                    return winner
            if index < len(remote_infos) and (
                    not tasks or loop.time() >= next_launch):
                tasks.append(loop.create_task(
                    _connect_one(loop, remote_infos[index], local_infos)))
                index += 1
                next_launch = loop.time() + happy_eyeballs_delay

        _raise_connect_errors(exceptions, all_errors)
    finally:
        for task in tasks:
            task.cancel()
        if tasks:
            # Native sock_connect's cancellation removes its registered
            # writer; gather is essential before the caller can return.
            results = await asyncio.gather(
                *tasks, return_exceptions=True)
            for loser_result in results:
                if isinstance(loser_result, socket.socket):
                    loser_result.close()
        if winner is not None and not succeeded:
            winner.close()
