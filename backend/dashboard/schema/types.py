from graphene_django import DjangoObjectType
from dashboard.models import RealGDP, NominalInflation, EconomicDataPoint
import graphene

class EconomicDataPointType(DjangoObjectType):
    class Meta:
        model = EconomicDataPoint
        fields = '__all__'

class RealGDPType(DjangoObjectType):
    economic_data_points = graphene.List(EconomicDataPointType)

    class Meta:
        model = RealGDP
        fields = '__all__'

    def resolve_economic_data_points(self, info):
        return self.economic_data_points.all().order_by('date')

class NominalInflationType(DjangoObjectType):
    economic_data_points = graphene.List(EconomicDataPointType)

    class Meta:
        model = NominalInflation
        fields = '__all__'

    def resolve_economic_data_points(self, info):
        return self.economic_data_points.all().order_by('date')

class QuadrantDataPointType(graphene.ObjectType):
    date = graphene.Date()
    gdp_growth = graphene.Float()
    inflation_growth = graphene.Float()
