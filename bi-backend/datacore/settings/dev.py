from datetime import timedelta
from .base import *

SECRET_KEY = env('SECRET_KEY')

DEBUG = True

ALLOWED_HOSTS = ["*"]

CORS_ALLOW_ALL_ORIGINS = True
CORS_ALLOWED_ORIGINS = [
    "https://biapi.tm-dev.xyz",
    "http://localhost:8080",
    "http://localhost:80",
    "http://localhost:3000",
    "http://localhost:5050",
    "http://localhost",
    "https://bi-frontend.vercel.app",
    "http://127.0.0.1"
]

# from corsheaders.defaults import default_headers
CORS_ALLOW_HEADERS = list(default_headers) + ["x-api-key"]

CSRF_TRUSTED_ORIGINS = CORS_ALLOWED_ORIGINS


# Database
DATABASES = {
    'default': {
        'ENGINE': env('DATABASE_ENGINE', None),
        'NAME': env('DATABASE_NAME', None),
        'USER': env('DATABASE_USER', None),
        'PASSWORD': env('DATABASE_PASSWORD', None),
        'HOST': env('DATABASE_HOST', None),
        'PORT': env('DATABASE_PORT', None),
    },
    'etl_db': {
        'ENGINE': env('DATABASE_ENGINE', None),
        'OPTIONS': {
            'options': f"-c search_path={env('ETL_DATABASE_SCHEMA', None)}"
        },
        'NAME': env('ETL_DATABASE_NAME', None),
        'USER': env('ETL_DATABASE_USER', None),
        'PASSWORD': env('ETL_DATABASE_PASSWORD', None),
        'HOST': env('ETL_DATABASE_HOST', None),
        'PORT': env('ETL_DATABASE_PORT', None),
        'CONN_MAX_AGE': int(env('ETL_CONN_MAX_AGE', None)),
    }

}
# https://docs.djangoproject.com/en/4.2/ref/settings/#databases

# Email
EMAIL_URL = env('EMAIL_URL', None)
X_API_KEY = env('X_API_KEY', None)
FRONTEND_URL = env('FRONTEND_URL', None)

# File download limit
FILE_DOWNLOAD_LIMIT = env('FILE_DOWNLOAD_LIMIT', None)

# Simple JWT
SIMPLE_JWT = {
    'ACCESS_TOKEN_LIFETIME': timedelta(days=1),
    'REFRESH_TOKEN_LIFETIME': timedelta(days=3),
    'UPDATE_LAST_LOGIN': True,
    'AUTH_HEADER_TYPES': ('Bearer', 'Token',),
}


# Logging
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '[{asctime}] {levelname} {module} {thread:d} - {message}',
            'style': '{',
            'datefmt': '%d-%m-%Y %H:%M:%S'
        },
    },
    'handlers': {
        'file': {
            'level': 'INFO',
            'class': 'logging.FileHandler',
            'filename': os.path.join(LOG_DIR, 'bi-backend.log'),
            'formatter': 'verbose',
        },
    },
    'root': {
        'handlers': ['file'],
        'level': 'INFO',
    },
    'loggers': {
        'django': {
            'handlers': ['file'],
            'level': 'INFO',
            'propagate': True,
        },
        'django.server': {
            'handlers': ['file'],
            'level': 'INFO',
            'propagate': True,
        },
        'django.request': {
            'handlers': ['file'],
            'level': 'INFO',
            'propagate': True,
        },
    },
}

# DEBUG TOOL
INTERNAL_IPS = ["127.0.0.1", ]

# CACHING

CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.redis.RedisCache',
        'LOCATION': f'{env('REDIS_URL')}/0',  # Redis database 0
    
        # 'TIMEOUT': 60 * 30  # 30 minutes
    },
    'secondary': {
        'BACKEND': 'django.core.cache.backends.redis.RedisCache',
        'LOCATION': f'{env('REDIS_URL')}/1',  # Redis database 1
   
        # 'TIMEOUT': 60 * 30  # 30 minutes
    },
}
CACHE_TIMEOUT = env('CACHE_TIMEOUT', None)


CELERY_BROKER_URL = env("CELERY_BROKER_URL")
CELERY_ACCEPT_CONTENT = ['application/json']
CELERY_RESULT_SERIALIZER='json'
CELERY_TASK_SERIALIZER='json'
CELERY_TIMEZONE = 'Africa/Lagos'

#store in db
CELERY_RESULT_BACKEND ='django-db'

# celery schedule

CELERY_BEAT_SCHEDULER = "django_celery_beat.schedulers:DatabaseScheduler"
