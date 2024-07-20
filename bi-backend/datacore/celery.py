from __future__ import absolute_import,unicode_literals
import os

from celery import Celery
from django.conf import settings

os.environ.setdefault("DJANGO_SETTINGS_MODULE",'datacore.settings.prod')
app= Celery('party')
app.conf.enable_utc= False
app.conf.update(timezone='Africa/Lagos')
app.config_from_object(settings,namespace='CELERY')

#celery Beat setting
app.autodiscover_tasks()

@app.task(bind=True)
def debug_task(self):
    print(f"Request: {self.request!r}")



