import base64
import calendar
import datetime
import json
import logging
import re
import secrets
from django.utils import timezone
import requests
from cryptography.fernet import Fernet
from django.conf import settings
from django.utils.crypto import get_random_string
from dateutil.relativedelta import relativedelta
import csv
from operator import attrgetter
from itertools import chain
from django.db.models import Q
from django.shortcuts import render
from django.http import HttpResponse
from django.core.mail import EmailMessage
import os
from account.models import *
from datacore.modules.email_template import send_download_link_for_report


def log_request(*args):
    for arg in args:
        logging.info(arg)


def format_phone_number(phone_number):
    phone_number = f"0{phone_number[-10:]}"
    return phone_number


def encrypt_text(text: str):
    key = base64.urlsafe_b64encode(settings.SECRET_KEY.encode()[:32])
    fernet = Fernet(key)
    secure = fernet.encrypt(f"{text}".encode())
    return secure.decode()


def decrypt_text(text: str):
    key = base64.urlsafe_b64encode(settings.SECRET_KEY.encode()[:32])
    fernet = Fernet(key)
    decrypt = fernet.decrypt(text.encode())
    return decrypt.decode()


def generate_random_password():
    return get_random_string(length=10)


def generate_random_otp():
    return get_random_string(length=6, allowed_chars="1234567890")


def get_previous_date(date, delta):
    previous_date = date - relativedelta(days=delta)
    return previous_date


def get_next_date(date, delta):
    next_date = date + relativedelta(days=delta)
    return next_date


def get_next_minute(date, delta):
    next_minute = date + relativedelta(minutes=delta)
    return next_minute


def get_previous_minute(date, delta):
    previous_minute = date - relativedelta(minutes=delta)
    return previous_minute


def get_previous_seconds(date, delta):
    # previous_seconds = date - datetime.timedelta(seconds=delta)
    previous_seconds = date - relativedelta(seconds=delta)
    return previous_seconds


def get_previous_hour(date, delta):
    previous_hour = date - relativedelta(hours=delta)
    return previous_hour


def get_day_start_and_end_datetime(date_time):
    day_start = date_time - relativedelta(day=0)
    # day_end = day_start + relativedelta(day=0)
    day_end = day_start + relativedelta(days=1)
    day_start = day_start.date()
    # day_start = datetime.datetime.combine(day_start.date(), datetime.time.min)
    # day_end = datetime.datetime.combine(day_end.date(), datetime.time.max)
    day_end = day_end.date()
    return day_start, day_end


def get_week_start_and_end_datetime(date_time):
    week_start = date_time - datetime.timedelta(days=date_time.weekday())
    week_end = week_start + datetime.timedelta(days=6)
    week_start = datetime.datetime.combine(week_start.date(), datetime.time.min)
    week_end = datetime.datetime.combine(week_end.date(), datetime.time.max)
    return week_start, week_end


def get_month_start_and_end_datetime(date_time):
    month_start = date_time.replace(day=1)
    month_end = month_start.replace(
        day=calendar.monthrange(month_start.year, month_start.month)[1]
    )
    month_start = datetime.datetime.combine(month_start.date(), datetime.time.min)
    month_end = datetime.datetime.combine(month_end.date(), datetime.time.max)
    return month_start, month_end


def get_year_start_and_end_datetime(date_time):
    year_start = date_time.replace(day=1, month=1, year=date_time.year)
    year_end = date_time.replace(day=31, month=12, year=date_time.year)
    year_start = datetime.datetime.combine(year_start.date(), datetime.time.min)
    year_end = datetime.datetime.combine(year_end.date(), datetime.time.max)
    return year_start, year_end


def get_previous_month_date(date, delta):
    return date - relativedelta(months=delta)


def get_next_month_date(date, delta):
    return date + relativedelta(months=delta)


def send_email(content, email, subject):
    payload = json.dumps({"Message": content, "address": email, "Subject": subject})
    response = requests.request(
        "POST",
        settings.EMAIL_URL,
        headers={"Content-Type": "application/json"},
        data=payload,
    )
    # log_request(f"Email sent to: {email}")
    log_request(f"Sending email to: {email}, Response: {response.text}")
    return response.text


def incoming_request_checks(request, require_data_field: bool = True) -> tuple:
    try:
        x_api_key = request.headers.get("X-Api-Key", None) or request.META.get(
            "HTTP_X_API_KEY", None
        )
        request_type = request.data.get("requestType", None)
        data = request.data.get("data", {})

        if not x_api_key:
            return False, "Missing or Incorrect Request-Header field 'X-Api-Key'"

        if x_api_key != settings.X_API_KEY:
            return False, "Invalid value for Request-Header field 'X-Api-Key'"

        if not request_type:
            return False, "'requestType' field is required"

        if request_type != "inbound":
            return False, "Invalid 'requestType' value"

        if require_data_field:
            if not data:
                return (
                    False,
                    "'data' field was not passed or is empty. It is required to contain all request data",
                )

        return True, data
    except (Exception,) as err:
        return False, f"{err}"


def get_incoming_request_checks(request) -> tuple:
    try:
        x_api_key = request.headers.get("X-Api-Key", None) or request.META.get(
            "HTTP_X_API_KEY", None
        )

        if not x_api_key:
            return False, "Missing or Incorrect Request-Header field 'X-Api-Key'"

        if x_api_key != settings.X_API_KEY:
            return False, "Invalid value for Request-Header field 'X-Api-Key'"

        return True, ""
        # how do I handle requestType and also client ID e.g 'inbound', do I need to expect it as a query parameter.
    except (Exception,) as err:
        return False, f"{err}"


def api_response(message, status: bool, data=None, **kwargs) -> dict:
    if data is None:
        data = {}
    try:
        reference_id = secrets.token_hex(30)
        response = dict(
            requestTime=timezone.now(),
            requestType="outbound",
            referenceId=reference_id,
            status=status,
            message=message,
            data=data,
            **kwargs,
        )

        # if "accessToken" in data and 'refreshToken' in data:
        if "accessToken" in data:
            # Encrypting tokens to be
            response["data"]["accessToken"] = encrypt_text(text=data["accessToken"])
            # response['data']['refreshToken'] = encrypt_text(text=data['refreshToken'])
            logging.info(msg=response)

            response["data"]["accessToken"] = decrypt_text(text=data["accessToken"])
            # response['data']['refreshToken'] = encrypt_text(text=data['refreshToken'])

        else:
            logging.info(msg=response)

        return response
    except (Exception,) as err:
        return err


def password_checker(password: str):
    try:
        # Python program to check validation of password
        # Module of regular expression is used with search()

        flag = 0
        while True:
            if len(password) < 8:
                flag = -1
                break
            elif not re.search("[a-z]", password):
                flag = -1
                break
            elif not re.search("[A-Z]", password):
                flag = -1
                break
            elif not re.search("[0-9]", password):
                flag = -1
                break
            elif not re.search("[#!_@$-]", password):
                flag = -1
                break
            elif re.search("\s", password):
                flag = -1
                break
            else:
                flag = 0
                break

        if flag == 0:
            return True, "Valid Password"

        return (
            False,
            "Password must contain uppercase, lowercase letters, '# ! - _ @ $' special characters "
            "and 8 or more characters",
        )
    except (Exception,) as err:
        return False, f"{err}"


def validate_email(email):
    try:
        regex = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
        if re.fullmatch(regex, email):
            return True
        return False
    except (TypeError, Exception) as err:
        # Log error
        return False


def calculate_percentage_change(old_day_count, new_day_count):
    if old_day_count == 0:
        return 0

    percentage_change = ((new_day_count - old_day_count) / old_day_count) * 100
    return percentage_change


def generate_csv(serialized_data, model_name):
    # Get the model fields and extract their names
    fields = model_name._meta.fields
    header = [field.name for field in fields]

    # Create a CSV file in memory
    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = (
        f"attachment; filename={model_name.__name__.lower()}_data.csv"
    )

    csv_writer = csv.writer(response)

    # Write the header
    csv_writer.writerow(header)

    # Write the serialized data
    try:
        for item in serialized_data:
            row_data = [
                item.get(field, "") for field in header
            ]  # Use item.get() to handle missing keys
            csv_writer.writerow(row_data)
    except Exception as e:
        print(f"Error writing to CSV: {e}")

    return response


def get_all_model_with_arranged():
    institutions = Institution.objects.filter(status=False)

    transaction_types = TransactionType.objects.filter(status=False)

    terminal_ids = TerminalId.objects.filter(status=False)

    channels = Channel.objects.filter(status=False)

    terminal_conditions = TerminalCondition.objects.filter(status=False)

    schema_names = SchemaName.objects.filter(status=False)

    countries = Country.objects.filter(status=False)

    currencies = Currency.objects.filter(status=False)

    extra_parameters = ExtraParameters.objects.filter(status=False)

    card_statuses = CardStatus.objects.filter(status=False)

    # Combine the QuerySets into a single list
    combined_list = list(
        institutions
        | transaction_types
        | terminal_ids
        | channels
        | terminal_conditions
        | schema_names
        | countries
        | currencies
        | extra_parameters
        | card_statuses
    )

    # sorting by 'createdOn

    combined_list_sorted = sorted(combined_list, key=attrgetter("createdOn"))
    return combined_list_sorted


def approval_request(instance):
    if instance.modelName == "institution":
        if instance.action == "create":
            user = User.objects.get(id=instance.data["createdBy"])
            instance.data["createdBy"] = user
            instance.data["bin"] = []
            Institution.objects.create(**instance.data)
        else:
            institution_instance = Institution.objects.get(id=instance.fieldId)
            for key, value in instance.data.items():
                setattr(institution_instance, key, value)
            institution_instance.save()

    elif instance.modelName == "country":
        if instance.action == "create":
            Country.objects.create(**instance.data)
        else:
            country_instance = Country.objects.get(id=instance.fieldId)
            for key, value in instance.data.items():
                setattr(country_instance, key, value)
            country_instance.save()

    elif instance.modelName == "currency":
        if instance.action == "create":
            Currency.objects.create(**instance.data)
        else:
            currency_instance = Currency.objects.get(id=instance.fieldId)
            for key, value in instance.data.items():
                setattr(currency_instance, key, value)
            currency_instance.save()

    elif instance.modelName == "channel":
        if instance.action == "create":
            Channel.objects.create(**instance.data)
        else:
            channel_instance = Channel.objects.get(id=instance.fieldId)
            for key, value in instance.data.items():
                setattr(channel_instance, key, value)
            channel_instance.save()
    elif instance.modelName == "schema":
        if instance.action == "create":
            SchemaName.objects.create(**instance.data)
        else:
            schema_instance = SchemaName.objects.get(id=instance.fieldId)
            for key, value in instance.data.items():
                setattr(schema_instance, key, value)
            schema_instance.save()


def customPagination(request, data, count):
    page = int(request.GET.get("page", "1"))
    next = page + 1
    prev = page - 1
    page = page * 50
    start_page = page - 50
    if count > 10000:
        count = 10000
    result = {
        "next": next,
        "previous": prev,
        "count": count,
        "results": data,
    }
    return result
