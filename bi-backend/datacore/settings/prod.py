from datetime import timedelta
from .base import *

SECRET_KEY = env("SECRET_KEY")

DEBUG = True
REDIS_URL = env("REDIS_URL")

ALLOWED_HOSTS = ["*"]

CORS_ALLOWED_ORIGINS = [
    "http://localhost:8080",
    "http://localhost:80",
    "http://localhost:3000",
    "http://localhost",
    "http://127.0.0.1",
    "http://196.46.20.44",
    "http://172.19.4.13",
]

CSRF_TRUSTED_ORIGINS = CORS_ALLOWED_ORIGINS

# Database
DATABASES = {
    "default": {
        "ENGINE": env("DATABASE_ENGINE", None),
        "OPTIONS": {"options": f"-c search_path={env('DATABASE_SCHEMA', None)}"},
        "NAME": env("DATABASE_NAME", None),
        "USER": env("DATABASE_USER", None),
        "PASSWORD": env("DATABASE_PASSWORD", None),
        "HOST": env("DATABASE_HOST", None),
        "PORT": env("DATABASE_PORT", None),
    },
    "etl_db": {
        "ENGINE": env("DATABASE_ENGINE", None),
        "OPTIONS": {"options": f"-c search_path={env('ETL_DATABASE_SCHEMA', None)}"},
        "NAME": env("ETL_DATABASE_NAME", None),
        "USER": env("ETL_DATABASE_USER", None),
        "PASSWORD": env("ETL_DATABASE_PASSWORD", None),
        "HOST": env("ETL_DATABASE_HOST", None),
        "PORT": env("ETL_DATABASE_PORT", None),
        "CONN_MAX_AGE": int(env("ETL_CONN_MAX_AGE", None)),
    },
}
# https://docs.djangoproject.com/en/4.2/ref/settings/#databases

# Email
EMAIL_URL = env("EMAIL_URL", None)
X_API_KEY = env("X_API_KEY", None)
FRONTEND_URL = env("FRONTEND_URL", None)

# File download limit
FILE_DOWNLOAD_LIMIT = env("FILE_DOWNLOAD_LIMIT", None)

# Simple JWT
SIMPLE_JWT = {
    "ACCESS_TOKEN_LIFETIME": timedelta(days=1),
    "REFRESH_TOKEN_LIFETIME": timedelta(days=2),
    "UPDATE_LAST_LOGIN": True,
    "AUTH_HEADER_TYPES": (
        "Bearer",
        "Token",
    ),
}

# Logging
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "[{asctime}] {levelname} {module} {thread:d} - {message}",
            "style": "{",
            "datefmt": "%d-%m-%Y %H:%M:%S",
        },
    },
    "handlers": {
        "file": {
            "level": "INFO",
            "class": "logging.handlers.TimedRotatingFileHandler",
            "filename": os.path.join(LOG_DIR, "bi-backend.log"),
            "when": "midnight",
            "interval": 1,  # daily
            "backupCount": 7,  # Keep logs for 7 days
            "formatter": "verbose",
        }
    },
    "root": {
        "handlers": ["file"],
        "level": "INFO",
    },
    "loggers": {
        "django": {
            "handlers": ["file"],
            "level": "INFO",
            "propagate": True,
        },
        "django.server": {
            "handlers": ["file"],
            "level": "INFO",
            "propagate": True,
        },
        "django.request": {
            "handlers": ["file"],
            "level": "INFO",
            "propagate": True,
        },
    },
}


# CACHES = {
#     'default': {
#         'BACKEND': 'django.core.cache.backends.redis.RedisCache',
#         'LOCATION': env('REDIS_URL', None),
#         # 'TIMEOUT': 60 * 30
#     }
# }

CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.redis.RedisCache",
        "LOCATION": f"{REDIS_URL}/0",  # Redis database 0
        # 'TIMEOUT': 60 * 30  # 30 minutes
    },
    "secondary": {
        "BACKEND": "django.core.cache.backends.redis.RedisCache",
        "LOCATION": f"{REDIS_URL}/1",  # Redis database 1
        # 'TIMEOUT': 60 * 30  # 30 minutes
    },
}

CACHE_TIMEOUT = env("CACHE_TIMEOUT", None)
