import graphene
from dashboard.schema import Query

schema = graphene.Schema(query=Query)