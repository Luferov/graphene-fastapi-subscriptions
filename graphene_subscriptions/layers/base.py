from abc import ABC, abstractmethod
from typing import Dict, Optional, Callable


class BaseChannelLayer(ABC):
    """Base channel layer."""

    MAX_NAME_LENGTH: int = 100

    def __init__(
        self,
        prefix: str,
        expiry: int = 60,
        capacity: int = 100,
        channel_capacity: int = None,
        on_disconnect: Optional[Callable] = None,
    ) -> None:
        """Initialization of BaseChannelLayer."""
        self.prefix = prefix
        self.expiry = expiry
        self.capacity = capacity
        self.channel_capacity = channel_capacity or {}
        self.on_disconnect = on_disconnect

    def get_group_channel_name(self, group: str) -> str:
        """Return the channel name used by a group.

        Includes '__group__' in the returned string so that these names are distinguished
        from those returned by `new_channel()`.

        Technically collisions are possible, but it takes what I believe is intentional
        abuse in order to have colliding names.
        """
        return f'{self.prefix}__group__{group}'

    def valid_group_name(self, name: str) -> bool:
        """Validate group name."""
        return True

    def valid_channel_name(self, name: str, receive: bool = False) -> bool:
        """Validate channel name."""
        return True

    @abstractmethod
    async def send(self, channel: str, message: Dict) -> None:
        """Send message to channel."""
        ...

    @abstractmethod
    async def receive(self, channel: str) -> None:
        """Receive message."""
        ...

    @abstractmethod
    async def flush(self):
        """Flush groups & channels."""
        ...

    @abstractmethod
    async def group_add(self, group: str, channel: str) -> None:
        """Add a new group."""
        ...

    @abstractmethod
    async def group_discard(self, group: str, channel: str) -> None:
        """Remove channel from group."""
        ...

    async def group_send(self, group: str, message: Dict):
        """Send message to group."""
        ...
