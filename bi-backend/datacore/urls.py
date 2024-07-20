from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from django.http.response import JsonResponse


def homepage(request):
    return JsonResponse({"message": "Welcome to BI Project Backend"})


urlpatterns = [
    path('', homepage),
    path('admin/', admin.site.urls),
    path('report/', include("report.urls")),
    path('account/', include("account.urls")),
    path('debug/', include("debug_toolbar.urls")),
    path('insight/', include("insight_module.urls")),

]

urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)

