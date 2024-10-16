import graphene
from .queries import Query as DashboardQuery

class Query(DashboardQuery, graphene.ObjectType):
    pass