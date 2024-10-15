import graphene
import dashboard.schema

class Query(dashboard.schema.Query, graphene.ObjectType):
    # This class will inherit from multiple Queries
    pass

schema = graphene.Schema(query=Query)