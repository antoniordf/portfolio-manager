from rest_framework import serializers

class QuadrantDataPointSerializer(serializers.Serializer):
    date = serializers.DateField()
    gdp_growth = serializers.FloatField()
    inflation_growth = serializers.FloatField()