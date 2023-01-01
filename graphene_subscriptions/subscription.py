"""Module for subscriptions."""
import asyncio
import collections
from typing import Dict, Type, Callable, Optional, Union, Sequence

import graphene.utils.props
from graphene import ObjectType, Argument, Field
from graphene.types.objecttype import ObjectTypeOptions
from graphene.types.utils import yank_fields_from_attrs
from graphene.utils.get_unbound_function import get_unbound_function
from graphql import GraphQLResolveInfo

from .layers import InMemoryChannelLayer, RedisPubSubChannelLayer


class SubscriptionOptions(ObjectTypeOptions):
    """Option stored in the Subscriptions `_meta` field."""

    arguments: Optional[Dict[str, Argument]] = None
    output: Optional[Type[ObjectType]] = None
    resolver: Optional[Callable] = None

    # Subscribe function
    group_name: Optional[Callable] = None
    subscribe: Optional[Callable] = None
    publish: Optional[Callable] = None
    unsubscribe: Optional[Callable] = None

    # channel layer: InMemory, RedisPubSubChannelLayer
    # default: InMemoryChannel Layer
    channel_layer: Callable = InMemoryChannelLayer


class Subscription(ObjectType):
    """Subscription type definition."""

    class Meta:
        """Meta class definition."""

        abstract = True

    # Return this from the `publish` to suppress the notification.
    SKIP = object()

    ################################################################################
    # Implementation
    ################################################################################

    @classmethod
    def __init_subclass_with_meta__(
        cls,
        arguments=None,
        output=None,
        resolver=None,
        group_name=None,
        subscribe=None,
        publish=None,
        unsubscribe=None,
        channel_layer=None,
        _meta=None,
        **options,
    ):
        assert callable(channel_layer), (
            'Channel layer must be callable and return type of: InMemoryChannelLayer, RedisPubSubChannelLayer'
        )
        if not _meta:
            _meta = SubscriptionOptions(cls)
        # Output class
        output = output or getattr(cls, 'Output', None)
        # Collect fields if output class is not explicitly defined.
        fields = {}
        if output is None:
            fields = collections.OrderedDict()
            for base in reversed(cls.__mro__):
                fields.update(yank_fields_from_attrs(base.__dict__, _as=Field))
            output = cls
        if not arguments:
            input_class = getattr(cls, 'Arguments', None)
            arguments = graphene.utils.props.props(input_class) if input_class else {}

        # Get `publish`, `subscribe`, and `unsubscribe`
        publish = publish or getattr(cls, 'publish', None)
        assert publish is not None, (
            f'Subscription `{cls.__qualname__}` does not define a method `publish`! '
            f'All subscription must define `publish` which processes a GraphQL query!'
        )
        group_name = group_name or getattr(cls, 'group_name', None)
        unsubscribe = unsubscribe or getattr(cls, 'unsubscribe', None)
        subscribe = subscribe or getattr(cls, 'subscribe', None)

        if _meta.fields:
            _meta.fields.update(fields)
        else:
            _meta.fields = fields

        _meta.arguments = arguments
        _meta.output = output
        _meta.resolver = get_unbound_function(cls._subscribe)
        _meta.publish = get_unbound_function(publish)
        _meta.group_name = get_unbound_function(group_name)
        _meta.subscribe = get_unbound_function(subscribe)
        _meta.unsubscribe = get_unbound_function(unsubscribe)
        _meta.channel_layer = channel_layer

        super(Subscription, cls).__init_subclass_with_meta__(_meta=_meta, **options)

    @classmethod
    async def _subscribe(cls, root, info: GraphQLResolveInfo, *args, **kwargs):
        """Subscription request received.

        This is called by the Graphene when a client subscribes.
        """
        group_name: str = cls._meta.group_name(root, info, *args, **kwargs)
        channel_layer: Union[InMemoryChannelLayer, RedisPubSubChannelLayer] = cls._meta.channel_layer()
        group_channel_name = channel_layer.get_group_channel_name(group_name)
        channel = await channel_layer.new_channel()
        # Add channel to group and subscribe to channel
        await channel_layer.group_add(group_channel_name, channel)

        # Present information
        if callable(cls._meta.subscribe):
            elements: Sequence = await cls._meta.subscribe(root, info, *args, **kwargs)
            if elements:
                for element in elements:
                    yield element

        try:
            # General loop
            while True:
                message = await channel_layer.receive(channel)
                publish_result = await cls._meta.publish(message, info, *args, **kwargs)
                if publish_result is None or publish_result == cls.SKIP:
                    continue
                yield publish_result
        finally:
            # Connection is close
            await cls._meta.unsubscribe(root, info, *args, **kwargs)
            await channel_layer.group_discard(group_channel_name, channel)

    ################################################################################
    # Field
    ################################################################################

    @classmethod
    def Field(
        cls,
        name: Optional[str] = None,
        description: Optional[str] = None,
        deprecation_reason: Optional[str] = None,
        required: bool = False
    ):
        """Represent subscription as a field to 'deploy' it."""
        return Field(
            cls._meta.output,
            args=cls._meta.arguments,
            resolver=cls._meta.resolver,
            name=name,
            description=description,
            deprecation_reason=deprecation_reason,
            required=required
        )

    @classmethod
    def broadcast(cls, group: str, payload: Dict):
        """Broadcast sync version."""
        asyncio.create_task(cls.broadcast_async(group, payload))

    @classmethod
    async def broadcast_async(cls, group: str, payload: Dict):
        """Async broadcast."""
        channel_layer = cls._meta.channel_layer()
        group_channel_name = channel_layer.get_group_channel_name(group)
        await channel_layer.group_send(group_channel_name, payload)
