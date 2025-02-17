import graphene

class FinancialDataPointType(graphene.ObjectType):
    date = graphene.String()   # or graphene.Date(), but typically a string is easier for the front-end
    open = graphene.Float()
    high = graphene.Float()
    low = graphene.Float()
    close = graphene.Float()
    volume = graphene.Float()

class QuadrantDataPointType(graphene.ObjectType):
    date = graphene.Date()
    gdp_growth = graphene.Float()
    inflation_growth = graphene.Float()
