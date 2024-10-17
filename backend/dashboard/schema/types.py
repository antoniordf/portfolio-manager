from graphene_django import DjangoObjectType
from dashboard.models import RealGDP, NominalInflation, DataPoint
import graphene

class DataPointType(DjangoObjectType):
    class Meta:
        model = DataPoint
        fields = '__all__'

class RealGDPType(DjangoObjectType):
    class Meta:
        model = RealGDP
        fields = '__all__'  # Alternatively, list the fields you want to expose

    # Resolve data_points field
    def resolve_data_points(self, info):
        return self.data_points.all().order_by('date')

class NominalInflationType(DjangoObjectType):
    class Meta:
        model = NominalInflation
        fields = '__all__'

class QuadrantDataPointType(graphene.ObjectType):
    date = graphene.Date()
    gdp_growth = graphene.Float()
    inflation_growth = graphene.Float()

