import graphene
import dashboard.schema 

class Query(
    dashboard.schema.Query,
    graphene.ObjectType,  # Add other app queries here
):
    pass

schema = graphene.Schema(query=Query)