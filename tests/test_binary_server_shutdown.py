import asyncio
from types import SimpleNamespace

import pytest

from asyncua.server.binary_server_asyncio import BinaryServer


@pytest.mark.asyncio
async def test_stop_drains_tasks_created_while_server_closes():
    class Transport:
        def close(self):
            pass

    iserver = SimpleNamespace(asyncio_transports=[Transport()])
    binary_server = BinaryServer(iserver, "127.0.0.1", 0, object())

    class SocketServer:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

        async def wait_closed(self):
            assert self.closed

            async def close_connection():
                await asyncio.sleep(0)

            binary_server.closing_tasks.append(asyncio.create_task(close_connection()))
            await asyncio.sleep(0)

    binary_server._server = SocketServer()

    await binary_server.stop()

    assert binary_server.closing_tasks == []
