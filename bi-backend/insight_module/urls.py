from django.urls import path
from .views import home
from .views import ViewDataInsightsView, InsightConfigView
app_name = 'insight_module'

urlpatterns = [
    path('', home, name="home"),
    path('view-insights/', ViewDataInsightsView.as_view(), name="view-insight"),
    path('fetch/', InsightConfigView.as_view(), name="fetch-insight"),  # GET
    path('set/<slug:slug>/', InsightConfigView.as_view(), name="fetch-insight"),  # PUT

]
