"""InMemory Data layer."""

import asyncio
import time
import uuid
from copy import deepcopy
from typing import Dict, Union, Tuple, Optional, Callable

from .base import BaseChannelLayer
from ..exceptions import ChannelFull

__all__: Tuple[str] = 'InMemoryChannelLayer'


class InMemoryChannelLayer(BaseChannelLayer):
    """In-memory channel layer implementation."""

    extensions = ['groups', 'flush']

    def __init__(
        self,
        prefix: str = 'inmemory',
        expiry: int = 60,
        group_expiry: int = 86400,
        capacity: int = 100,
        channel_capacity: int = None,
        on_disconnect: Optional[Callable] = None
    ) -> None:
        super().__init__(
            prefix,
            expiry=expiry,
            capacity=capacity,
            channel_capacity=channel_capacity,
            on_disconnect=on_disconnect
        )

        self.group_expiry = group_expiry
        self.channels = {}
        self.groups: Dict[str, Dict[str, Union[int, float]]] = {}

    async def new_channel(self, prefix: str = 'specific.') -> str:
        """Generate a new name for channel."""
        return f'{self.prefix}{prefix}inmemory!{uuid.uuid4().hex}'

    async def subscribe_to_channel(self, channel: str):
        """Subscribe to channel."""
        self.channels.setdefault(channel, asyncio.Queue())

    ################################################################################
    # Channel layer API
    ################################################################################

    async def send(self, channel: str, message: Dict):
        """Send message onto channel."""
        assert isinstance(message, dict), 'Message should be a dict'
        assert self.valid_channel_name(channel), 'Channel name not valid'
        # Make sure the message does not contain reserved keys
        assert '__asgi_channel__' not in message

        queue = self.channels.setdefault(channel, asyncio.Queue())
        # Очередь переполнена
        if queue.qsize() > self.capacity:
            raise ChannelFull(channel)

        # Put message (time_expiry, message)
        await queue.put((time.time() + self.expiry, message))

    async def receive(self, channel: str) -> Dict:
        """Receive the first message that arrives at the channel.

        If more than one coroutine waits on the same channel,
        a random one of the waiting coroutines will get the result.
        """
        assert self.valid_channel_name(channel), 'Channel name not valid'
        self._clean_expired()

        queue: asyncio.Queue = self.channels.setdefault(channel, asyncio.Queue())

        # Do a regular receive
        try:
            # _ - is time
            message: Dict
            _, message = await queue.get()
        finally:
            # Delete channel if queue is empty
            if queue.empty():
                del self.channels[channel]

        return message

    def _clean_expired(self):
        """Goes through all messages and groups and removes those that are expired.

        Any channels and group with an expired message is removed from all groups.
        """
        # Channel cleanup
        channel: str
        queue: asyncio.Queue
        for channel, queue in list(self.channels.items()):
            # _queue is element in queue
            while not queue.empty() and queue._queue[0][0] < time.time(): # noqa
                queue.get_nowait()
                # Any removal prompts group discard
                self._remove_from_groups(channel)
                # Is the channel is empty and should to delete?
                if queue.empty():
                    del self.channels[channel]

        # Group cleanup
        timeout: int = int(time.time()) - self.group_expiry
        for group in self.groups:
            for channel in list(self.groups.get(group, set())):
                if self.groups[group][channel] and int(self.groups[group][channel] < timeout):
                    # If join time is older than group_expiry and the group membership
                    del self.groups[group][channel]

    def _remove_from_groups(self, channel: str):
        """Remove a channel from all groups. Used when a message ot it expires."""
        for channels in self.groups.values():
            if channel in channels:
                del channels[channel]

    ################################################################################
    # Flush extension
    ################################################################################

    async def flush(self):
        """Flush groups & channels."""
        self.channels = {}
        self.groups = {}

    ################################################################################
    # Groups extension
    ################################################################################

    async def group_add(self, group: str, channel: str):
        """Add a new group."""
        # Check inputs
        assert self.valid_group_name(group), 'Group name not valid'
        assert self.valid_channel_name(channel), 'Channel name not valid'
        # Add to group dict
        self.groups.setdefault(group, {})
        self.groups[group][channel] = time.time()
        await self.subscribe_to_channel(channel)

    async def group_discard(self, group: str, channel: str):
        """Remove channel from group."""
        # Check inputs
        assert self.valid_group_name(group), 'Group name not valid'
        assert self.valid_channel_name(channel), 'Channel name not valid'
        # Remove from group set
        for group in self.groups:
            if channel in self.groups[group]:
                del self.groups[group][channel]
        # self.groups dict may be changed during iterable
        self.groups = {group: channels for group, channels in self.groups.items() if channels}

    async def group_send(self, group: str, message: Dict):
        """Send message to group."""
        assert isinstance(message, dict), 'Message should be a dict'
        assert self.valid_group_name(group), 'Group name not valid'
        self._clean_expired()
        for channel in self.groups.get(group, set()):
            try:
                await self.send(channel, deepcopy(message))
            except ChannelFull:
                pass
