from django.contrib import admin
from .models import (
    InsightAnalysisNotificationModel,
    InsightModel,
    InsightConfigModel,
    BackendConfiguration,
)
# Register your models here.

admin.site.register(InsightModel)
admin.site.register(InsightConfigModel)
admin.site.register(InsightAnalysisNotificationModel)
admin.site.register(BackendConfiguration)
