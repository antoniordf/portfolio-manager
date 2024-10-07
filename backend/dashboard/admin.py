from django.contrib import admin
from .models import RealGDP, TreasuryYield, DataSeries, DataPoint

admin.site.register(DataPoint)
admin.site.register(RealGDP)
admin.site.register(TreasuryYield)
