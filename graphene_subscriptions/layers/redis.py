"""Redis Data layer.

This layer based on with Redis Pub/Sub.
"""

import asyncio
import contextlib
import functools
import logging
import types
import uuid
from typing import Dict, Optional, List, Union, Callable, Set, Tuple

import aioredis
import aioredis.client
import aioredis.sentinel
import msgpack

from .base import BaseChannelLayer
from ..utils import consistent_hash

__all__: Tuple[str] = ('RedisPubSubChannelLayer',)


logger = logging.getLogger(__name__)


def _wrap_close(proxy: 'RedisPubSubChannelLayer', loop: asyncio.AbstractEventLoop):
    """Wrapped close functional.

    It needs to run loop.flush() when loop is closing.
    """
    original_impl = loop.close

    def _wrapper(self, *args, **kwargs):
        """Decoration function."""
        if loop in proxy.layers:
            layer = proxy.layers[loop]
            del proxy.layers[loop]
            loop.run_until_complete(layer.flush())

        self.close = original_impl
        return self.close(*args, **kwargs)  # noqa

    loop.close = types.MethodType(_wrapper, loop)


async def _async_proxy(obj: 'RedisPubSubChannelLayer', name: str, *args, **kwargs):
    """Async proxy.

    Must be defined as a function and not method dut to
    https://bugs.python.org/issue38364 is open.

    :param obj:
    :param name:
    :param args:
    :param kwargs:
    :return:
    """
    layer = obj.get_layer()
    return await getattr(layer, name)(*args, **kwargs)


class RedisPubSubChannelLayer:
    """Redis Pub/Sub channel layer."""

    def __init__(self, *args, **kwargs) -> None:
        self.args = args
        self.kwargs = kwargs
        self.layers = {}

    def __getattr__(self, name: str):
        """Override regular behavior."""
        if name in (
                'new_channel',
                'send',
                'receive',
                'group_add',
                'group_discard',
                'group_send',
                'flush'
        ):
            return functools.partial(_async_proxy, self, name)
        else:
            return getattr(self.get_layer(), name)

    @staticmethod
    def serialize(message: Dict):
        """Serialize message to a byte string."""
        assert isinstance(message, dict), 'Message should be a dict'
        return msgpack.packb(message)

    @staticmethod
    def deserialize(message: Union[bytes, int, float, memoryview]) -> Dict:
        """Deserialize message from a byte string."""
        return msgpack.unpackb(message)

    def get_layer(self):
        """Get current layout.


        The layer is anchored to the connection so that there are no intersections.
        """
        loop = asyncio.get_running_loop()

        try:
            layer = self.layers[loop]
        except KeyError:
            layer = RedisPubSubLoopLayer(
                *self.args,
                **self.kwargs,
                channel_layer=self
            )
            self.layers[loop] = layer
            # Add wrap close
            _wrap_close(self, loop)
        return layer


class RedisPubSubLoopLayer(BaseChannelLayer):
    """Channel layyer that uses Redis's Pub/Sub functionality."""

    extensions = ['groups', 'flush']

    def __init__(
            self,
            hosts: Optional[Union[Dict, List[str]]] = None,
            prefix: str = 'asgi',
            on_disconnect: Optional[Callable] = None,
            on_reconnect: Optional[Callable] = None,
            channel_layer: Optional[RedisPubSubChannelLayer] = None
    ) -> None:
        """Init Redis Pub/Sub loop layer."""
        super().__init__(prefix, on_disconnect=on_disconnect)
        if hosts is None:
            hosts = ['redis://localhost:6379/0']
        assert isinstance(hosts, list) and len(hosts) > 0, '`hosts` must be a list with at least one Redis server'
        self.on_reconnect = on_reconnect
        self.channel_layer: Optional[RedisPubSubChannelLayer] = channel_layer

        self.channels: Dict[str, asyncio.Queue] = {}
        self.groups: Dict[str, Set[str]] = {}
        self.shards: List[RedisSingleShardConnection] = [RedisSingleShardConnection(host, self) for host in hosts]

    def get_shard(self, channel_or_group_name: str):
        """Return the shard that is used exclusively for this channel or group."""
        return self.shards[consistent_hash(channel_or_group_name, len(self.shards))]

    async def subscribe_to_channel(self, channel: str):
        self.channels[channel] = asyncio.Queue()
        shard = self.get_shard(channel)
        await shard.subscribe(channel)

    ################################################################################
    # Channel layer API
    ################################################################################

    async def send(self, channel: str, message: Dict):
        """Send a message onto a (general or specific) channel."""
        shard: RedisSingleShardConnection = self.get_shard(channel)
        await shard.publish(channel, self.channel_layer.serialize(message))

    async def new_channel(self, prefix='specific.'):
        """Return a new channel name that can be used by a consumer in out process."""
        return f'{self.prefix}{prefix}redis!{uuid.uuid4().hex}'

    async def receive(self, channel: str) -> Dict:
        """Receive the first message that arrives at the channel.

        If more than one coroutine waits on the same channel, a random one
        of the waiting coroutines will get the result.
        """
        if channel not in self.channels:
            await self.subscribe_to_channel(channel)
        queue: asyncio.Queue = self.channels[channel]

        try:
            message = await queue.get()
        except (
            asyncio.CancelledError,
            asyncio.TimeoutError,
            GeneratorExit
        ):
            # Delete channel if exception was raise
            await self.delete_channel(channel)
            raise
        return self.channel_layer.deserialize(message)

    async def delete_channel(self, channel: str):
        """Delete channel."""
        if channel in self.channels:
            del self.channels[channel]
            try:
                shared: RedisSingleShardConnection = self.get_shard(channel)
                await shared.unsubscribe(channel)
            except BaseException as e:
                logger.exception(f'Unexpected exception while cleaning-up channel: {e}')
        # No need channel from group due to channel & group is the same
        # Delete channel from group
        # for group in self.groups:
        #    if channel in self.groups[group]:
        #        self.groups[group].remove(channel)

    ################################################################################
    # Flush extension
    ################################################################################

    async def flush(self):
        """Flush the layer, making it like new. It can continue to be used as if it
        was just created. This also closes connections, serving as a clean-up
        method; connections will be re-opened if you continue using this layer.
        """
        self.channels = {}
        self.groups = {}
        shard: RedisSingleShardConnection
        for shard in self.shards:
            await shard.flush()

    ################################################################################
    # Groups extension
    ################################################################################

    async def group_add(self, group: str, channel: str):
        """Add channel to group."""
        group_channel = self.get_group_channel_name(group)
        group_channels = self.groups.setdefault(group_channel, set())
        if channel not in group_channels:
            group_channels.add(channel)
        # Subscribe to group
        # Actually in this implementations group and channel is the same
        await self.subscribe_to_channel(channel)

        # shard = self.get_shard(group_channel)
        # await shard.subscribe(group_channel)

    async def group_discard(self, group: str, channel: str):
        """Remove channel from group."""
        group_channel = self.get_group_channel_name(group)
        group_channels = self.groups.get(group_channel, set())
        if channel not in group_channels:
            # Channel is not in group, do nothing
            return

        group_channels.remove(channel)
        shard = self.get_shard(channel)
        await shard.unsubscribe(channel)

        if not self.groups[group_channel]:
            # Delete group if empty
            del self.groups[group_channel]

            # If group is empty
            # shard = self.get_shard(group_channel)
            # await shard.unsubscribe(group_channel)

    async def group_send(self, group: str, message: Dict):
        """Send message to group."""
        group_channel = self.get_group_channel_name(group)
        group_channels: Set[str] = self.groups.setdefault(group_channel, set())

        for group_channel in group_channels:
            shard = self.get_shard(group_channel)
            await shard.publish(group_channel, self.channel_layer.serialize(message))

        # shard = self.get_shard(group_channel)
        # await shard.publish(group, self.channel_layer.serialize(message))


class RedisSingleShardConnection:
    """One shared connection to Redis server."""

    def __init__(self, host: Union[Dict[str, str], str], channel_layer: RedisPubSubLoopLayer) -> None:
        self.host = host.copy() if isinstance(host, dict) else {'address': host}
        self.master_name: Optional[str] = self.host.pop('master_name', None)
        self.sentinel_kwargs: Dict = self.host.pop('sentinel_kwargs', None)
        self.channel_layer: RedisPubSubLoopLayer = channel_layer
        self.subscribed_to: Set[str] = set()
        self.lock: asyncio.Lock = asyncio.Lock()
        self.redis: Optional[aioredis.Redis] = None
        self.pubsub: Optional[aioredis.client.PubSub] = None
        self.receive_task: Optional = None

    async def publish(self, channel, message: Union[bytes, str, int, float, memoryview]) -> None:
        async with self.lock:
            self.ensure_redis()
            await self.redis.publish(channel, message)

    async def subscribe(self, channel: str) -> None:
        async with self.lock:
            if channel not in self.subscribed_to:
                self.ensure_redis()
                self.ensure_receiver()
                await self.pubsub.subscribe(channel)
                self.subscribed_to.add(channel)

    async def unsubscribe(self, channel: str) -> None:
        async with self.lock:
            if channel in self.subscribed_to:
                self.ensure_redis()
                self.ensure_receiver()
                await self.pubsub.unsubscribe(channel)
                self.subscribed_to.remove(channel)

    async def flush(self) -> None:
        async with self.lock:
            if self.receive_task is not None:
                self.receive_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self.receive_task
                self.receive_task = None
            if self.redis is not None:
                # The pool was created just for this client, so make sure it is closed,
                # otherwise it will schedule the connection to be closed inside the
                # __del__ method, witch doesn't have a loop running anymore.
                await self.redis.close()
                self.redis = None
                self.pubsub = None
            self.subscribed_to = set()

    async def do_receiving(self) -> None:
        while True:
            try:
                if self.pubsub and self.pubsub.subscribed:
                    message = await self.pubsub.get_message(
                        ignore_subscribe_messages=True,
                        timeout=0.1
                    )
                    self.receive_message(message)
                else:
                    await asyncio.sleep(0.1)
            except (
                    asyncio.CancelledError,
                    asyncio.TimeoutError,
                    GeneratorExit
            ):
                raise
            except BaseException as e:
                logger.exception(f'Unexpected exception in receive task: {e}')
                await asyncio.sleep(1)

    def receive_message(self, message: Optional[Dict]) -> None:
        """Receive message.

        Shift the message from the Pub / Sub loop to the socket loop.
        """
        if message is not None:
            channel = message['channel']
            data = message['data']

            if isinstance(channel, bytes):
                channel = channel.decode()
            if channel in self.channel_layer.channels:
                self.channel_layer.channels[channel].put_nowait(data)
            elif channel in self.channel_layer.groups:
                for channel_name in self.channel_layer.groups[channel]:
                    self.channel_layer.channels[channel_name].put_nowait(data)

    def ensure_redis(self) -> None:
        """Ensure redis."""
        if self.redis is None:
            if self.master_name is None:
                pool = aioredis.ConnectionPool.from_url(self.host['address'])
            else:
                # aioredis default timeout is way too low
                pool = aioredis.sentinel.SentinelConnectionPool(
                    self.master_name,
                    aioredis.sentinel.Sentinel(
                        self.host['sentinels'],
                        socket_timeout=2,
                        sentinel_kwargs=self.sentinel_kwargs
                    )
                )
            self.redis = aioredis.Redis(connection_pool=pool)
            self.pubsub = self.redis.pubsub()

    def ensure_receiver(self) -> None:
        """Wrapped task in the Future."""
        if self.receive_task is None:
            self.receive_task = asyncio.ensure_future(self.do_receiving())
