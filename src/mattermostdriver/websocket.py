import typing
import asyncio
import json
import logging

import aiohttp

logger = logging.getLogger(__name__)


class Websocket:

    heartbeat: int = 5
    receive_timeout: int = 10
    keepalive_delay: float = 5

    def __init__(self, options: typing.Dict[str, typing.Any], token: str):
        self.options = options
        self._token = token
        self._alive = False

    async def connect(
        self,
        event_handler: typing.Callable[[str], typing.Awaitable[None]],
    ) -> None:

        url = "wss://{url:s}:{port:s}{basepath:s}/websocket".format(
            url=self.options["url"],
            port=str(self.options["port"]),
            basepath=self.options["basepath"],
        )

        self._alive = True

        while self._alive:
            try:
                async with aiohttp.ClientSession() as session:
                    # The receive_timeout parameter allows you not to block the cycle of receiving messages and throws an error
                    # TimeoutError(by the way, if you do not do _authenticate, then there will be no error, the loop will just end)
                    # after receive_timeout seconds if no messages have been received.
                    # At the same time, the heartbeat parameter ensures that every heartbeat of seconds should come
                    # at least a PONG message, and if it does not come, it means that the connection is broken and you need to
                    # recreate the connection
                    async with session.ws_connect(
                        url,
                        heartbeat=self.heartbeat,
                        receive_timeout=self.receive_timeout,
                        verify_ssl=self.options["verify"],
                    ) as websocket:
                        await self._authenticate(websocket)
                        async for message in websocket:
                            await event_handler(message.data)
            except Exception as e:
                logger.exception(
                    f"Failed to establish websocket connection: {type(e)} thrown",
                )
                await asyncio.sleep(self.keepalive_delay)

    def disconnect(self) -> None:
        logger.info("Disconnecting websocket")
        self._alive = False

    async def _authenticate(self, websocket: aiohttp.client.ClientWebSocketResponse) -> None:
        logger.info("Authenticating websocket")
        json_data = json.dumps(
            {
                "seq": 1,
                "action": "authentication_challenge",
                "data": {"token": self._token},
            },
        )
        await websocket.send_str(json_data)
