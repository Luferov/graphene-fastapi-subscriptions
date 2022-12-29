import graphene
from fastapi import FastAPI
from starlette_graphene3 import GraphQLApp, make_graphiql_handler

from .schema import Query, Mutation, Subscription


app = FastAPI()
schema = graphene.Schema(query=Query, mutation=Mutation, subscription=Subscription)

app.mount('/graphql/', GraphQLApp(schema, on_get=make_graphiql_handler()))
