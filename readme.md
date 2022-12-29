# Graphene FastAPI subscription

## Install

To install graphene, just run this command in your shell

```shell
pip install graphene-fastapi-subscription
```
or
Via poetry
```shell
postry add graphene-fastapi-subscription
```


## Examples

Create `schema.py` file

```python
from typing import Optional, Union
import asyncio
import graphene

from graphene_subscriptions.decorators import subscription_bind
from graphene_subscriptions.layers import RedisPubSubChannelLayer
from graphene_subscriptions.subscription import Subscription as _Subscription


channel_layer = RedisPubSubChannelLayer()


def get_channel_layer():
    return channel_layer


class User(graphene.ObjectType):
    id = graphene.ID()
    name = graphene.String()


class Query(graphene.ObjectType):
    me = graphene.Field(User)

    @staticmethod
    def resolve_me(root, info):
        return {"id": "john", "name": "John"}


class EchoMutation(graphene.Mutation):
    """Echo mutation."""

    class Arguments:
        chat_id = graphene.Int()
        message = graphene.String(required=True, description='Input you message')

    message = graphene.String(required=True, description='Input you message')

    @staticmethod
    def mutate(root, info, chat_id, message, **kwargs):
        # Broadcast to group
        MessageSubscription.broadcast(f'chat.${chat_id}', {'message': message})
        return EchoMutation(message=message)


class Mutation(graphene.ObjectType):
    echo_mutation = EchoMutation.Field()


class MessageSubscription(_Subscription):

    class Meta:
        # If in memory, it should be Singleton
        channel_layer = get_channel_layer

    class Arguments:
        chat_id = graphene.Int()

    message = graphene.String()

    @staticmethod
    def group_name(root, info, chat_id: int, *args, **kwargs) -> Optional[str]:
        """Subscribe to group."""
        return f'chat.${chat_id}'

    @staticmethod
    def pre_start(root, info, chat_id, *args, **kwargs):
        """Send MessageSubscription before start."""
        return [MessageSubscription(message='Message from pre')]

    @staticmethod
    def publish(payload, info, chat_id: int, *args, **kwargs) -> Union['MessageSubscription', 'MessageSubscription.SKIP']:
        """Receive payload."""
        message: str = payload.get('message', MessageSubscription.SKIP)
        if message == MessageSubscription.SKIP or message == 'skip':
            return MessageSubscription.SKIP
        return MessageSubscription(message=payload.get('message'))

    @staticmethod
    def unsubscribe(root, info, chat_id: int, *args, **kwargs):
        """Unsubscribe from channel."""
        print(f'Unsubscribe from: {chat_id}')


@subscription_bind
class Subscription(graphene.ObjectType):
    count = graphene.Int(upto=graphene.Int())

    message = MessageSubscription.Field(required=True)

    @staticmethod
    async def subscribe_count(root, info, upto=3, *args, **kwargs):
        for i in range(upto):
            yield i
            await asyncio.sleep(1)
```

The `app.py` is as simple as:
```python

import graphene
from fastapi import FastAPI
from starlette_graphene3 import GraphQLApp, make_graphiql_handler

from .schema import Query, Mutation, Subscription

app = FastAPI()
schema = graphene.Schema(query=Query, mutation=Mutation, subscription=Subscription)

app.mount('/graphql/', GraphQLApp(schema, on_get=make_graphiql_handler()))

```