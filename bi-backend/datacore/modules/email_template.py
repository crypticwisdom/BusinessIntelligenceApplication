from django.shortcuts import render
from django.conf import settings
import os
import csv


def account_opening_email(profile, password):
    from datacore.modules.utils import send_email

    first_name = profile.user.first_name
    email = profile.user.email
    if not profile.user.first_name:
        first_name = "BI Admin"

    message = (
        f"Dear {first_name}, <br><br>Welcome to <a href='{settings.FRONTEND_URL}' target='_blank'>"
        f"PayArena Business Inteligence Dashboard.</a><br>Please see below, your username "
        f"and password. You will be required to change your password on your first login <br><br>"
        f"username: <strong>{email}</strong><br>password: <strong>{password}</strong>"
    )
    subject = "BI Registration"
    contents = render(
        None, "default_template.html", context={"message": message}
    ).content.decode("utf-8")
    send_email(contents, email, subject)
    return True


def send_token_to_email(user_profile):
    from datacore.modules.utils import send_email, decrypt_text

    first_name = user_profile.user.first_name
    if not user_profile.user.first_name:
        first_name = "BI Admin"
    email = user_profile.user.email
    decrypted_token = decrypt_text(user_profile.otp)

    message = (
        f"Dear {first_name}, <br><br>Kindly use the below One Time Token, to complete your action<br><br>"
        f"OTP: <strong>{decrypted_token}</strong>"
    )
    subject = "Business Intelligence OTP"
    contents = render(
        None, "default_template.html", context={"message": message}
    ).content.decode("utf-8")
    send_email(contents, email, subject)
    return True


def send_forgot_password_token_to_email(user):
    from datacore.modules.utils import send_email

    first_name = user.first_name
    email = user.email
    url = f"{settings.FRONTEND_URL}/forgot-password?otp={user.profile.otp}"
    message = (
        f"Dear {first_name}, <br><br>Kindly use the link below to Change your password<br><br>"
        f"URL: <a href='{url}'>link</a>"
    )
    subject = "BI Forgot Password"
    contents = render(
        None, "default_template.html", context={"message": message}
    ).content.decode("utf-8")
    send_email(contents, email, subject)
    return True


# def site_performance_alert(email, performance, value, inst_name, second):
#     name = f"{inst_name} Administrator"
#     metric = "response time"
#     if performance == "approvalRate":
#         metric = "approval rate"
#
#     message = f"Dear {name}, <br><br>The {metric} of your inbound transfer service was " \
#               f"<strong>{value}%</strong> within the last {second} seconds.<br>" \
#               f"Kindly review the service for optimal performance<br><br>"
#     subject = "PayArena BI Transfer Service Performance Monitor"
#     contents = render(None, 'default_template.html', context={'message': message}).content.decode('utf-8')
#     send_email(contents, email, subject)
#     return True


# def route_trigger_alert(email, route):
#     message = f"Dear Administrator, <br><br>Today's transaction amount for <strong>{route}</strong> " \
#               f"route has exceeded the configured value"
#     subject = "PayArena BI Route Trigger"
#     contents = render(None, 'default_template.html', context={'message': message}).content.decode('utf-8')
#     send_email(contents, email, subject)
#     return True


def send_download_link_for_report(user, link, email):
    from datacore.modules.utils import send_email

    first_name = user.first_name
    email = email
    if not user.first_name:
        first_name = "BI Admin"

    message = (
        f"Dear {first_name}, <br><br>Kindly click on the below link to download your requested report. <br>"
        f"<p/>Click the button below to download the file <p/>"
        f"<div style='text-align:left'><a href='{link}' target='_blank' "
        f"style='background-color: #67C1F0; color: white; padding: 15px 25px; text-align: center; "
        f"text-decoration: none; display: inline-block;'>Download</a></div><br>"
    )
    subject = "Report Download"
    contents = render(
        None, "default_template.html", context={"message": message}
    ).content.decode("utf-8")
    send_email(contents, email, subject)
    return True


# def generate_and_send_csv(request, queryset, model_name, recipient_email):
#     print(queryset)
#     from datacore.modules.utils import send_email

#     # Get the model fields and extract their names
#     # Get the model fields and extract their names
#     fields = model_name._meta.get_fields()
#     header = [field.name for field in fields]

#     # Create a CSV file in media directory
#     media_path = os.path.join(settings.MEDIA_ROOT, "csv_files")
#     os.makedirs(media_path, exist_ok=True)
#     csv_file_path = os.path.join(media_path, f"{model_name.__name__.lower()}_data.csv")

#     with open(csv_file_path, "w", newline="") as csv_file:
#         csv_writer = csv.writer(csv_file)
#         csv_writer.writerow(header)

#         try:
#             for obj in queryset:
#                 row_data = [getattr(obj, field) for field in header]
#                 csv_writer.writerow(row_data)
#         except:
#             for obj in queryset:
#                 row_data = [
#                     obj.get(field, "") for field in header
#                 ]  # Use obj.get() with a default value
#                 csv_writer.writerow(row_data)

#     # Construct the download link
#     base_url = settings.BASE_URL.rstrip("/")
#     relative_csv_path = os.path.relpath(csv_file_path, settings.MEDIA_ROOT)
#     download_link = f"{base_url}/media/csv_files/{model_name.__name__.lower()}_data.csv"
#     print(download_link)
#     first_name = request.user.first_name
#     email = recipient_email
#     if not request.user.first_name:
#         first_name = "BI Admin"


#     message = (
#         f"Dear {first_name}, <br><br>Kindly click on the below link to download your requested report. <br>"
#         f"<p/>Click the button below to download the file <p/>"
#         f"<div style='text-align:left'><a href='{download_link}' target='_blank' "
#         f"style='background-color: #67C1F0; color: white; padding: 15px 25px; text-align: center; "
#         f"text-decoration: none; display: inline-block;'>Download</a></div><br>"
#     )
#     subject = "Report Download"
#     contents = render(
#         None, "default_template.html", context={"message": message}
#     ).content.decode("utf-8")
#     send_email(contents, email, subject)
#     return "Email sent successfully."
def generate_and_send_csv(request, serialized_data, model_name, recipient_email):
    print(serialized_data)
    from datacore.modules.utils import send_email

    # Get the model fields and extract their names
    fields = model_name._meta.get_fields()
    header = [field.name for field in fields]

    # Create a CSV file in the media directory
    media_path = os.path.join(settings.MEDIA_ROOT, "csv_files")
    os.makedirs(media_path, exist_ok=True)
    csv_file_path = os.path.join(media_path, f"{model_name.__name__.lower()}_data.csv")

    with open(csv_file_path, "w", newline="") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(header)

        for item in serialized_data:
            row_data = [
                item.get(field, "") for field in header
            ]  # Use item.get() for safe access
            csv_writer.writerow(row_data)

    # Construct the download link
    base_url = settings.BASE_URL.rstrip("/")
    relative_csv_path = os.path.relpath(csv_file_path, settings.MEDIA_ROOT)
    download_link = f"{base_url}/media/csv_files/{model_name.__name__.lower()}_data.csv"
    print(download_link)

    first_name = request.user.first_name or "BI Admin"
    message = (
        f"Dear {first_name}, <br><br>Kindly click on the below link to download your requested report. <br>"
        f"<p/>Click the button below to download the file <p/>"
        f"<div style='text-align:left'><a href='{download_link}' target='_blank' "
        f"style='background-color: #67C1F0; color: white; padding: 15px 25px; text-align: center; "
        f"text-decoration: none; display: inline-block;'>Download</a></div><br>"
    )
    subject = "Report Download"
    contents = render(
        None, "default_template.html", context={"message": message}
    ).content.decode("utf-8")
    send_email(contents, recipient_email, subject)

    return "Email sent successfully."


def extract_transaction_data(queryset, fields):
    extracted_data = []
    for item in queryset:
        row = {field: getattr(item, field, "") for field in fields}
        extracted_data.append(row)
    return extracted_data


def generate_and_send_csv_other(request, queryset, model_name, recipient_email):
    print(queryset, "pppp")
    from datacore.modules.utils import send_email

    # Get the model fields and extract their names
    fields = model_name._meta.get_fields()
    header = [field.name for field in fields]

    # Specify the fields you are interested in
    transaction_fields = [
        "id",
        "department_table_id",
        "department",
        "transaction_type",
        "transaction_time",
        "channel",
        "aquirer_terminal_id",
        "pos_id",
        "transaction_ref",
        "card_no",
        "amount",
        "amount_due",
        "amount_paid",
        "transaction_status",
        "aquirer",
        "aquirer_fee",
        "created",
        "last_updated",
        "value_chain",
        "particulars",
        "agency_code",
    ]

    # Extract transaction data from the queryset
    extracted_data = extract_transaction_data(queryset, transaction_fields)

    # Create a CSV file in the media directory
    media_path = os.path.join(settings.MEDIA_ROOT, "csv_files")
    os.makedirs(media_path, exist_ok=True)
    csv_file_path = os.path.join(media_path, f"{model_name.__name__.lower()}_data.csv")

    with open(csv_file_path, "w", newline="") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(transaction_fields)

        for row_data in extracted_data:
            csv_writer.writerow([row_data[field] for field in transaction_fields])

    # Construct the download link
    base_url = settings.BASE_URL.rstrip("/")
    relative_csv_path = os.path.relpath(csv_file_path, settings.MEDIA_ROOT)
    download_link = f"{base_url}/media/csv_files/{model_name.__name__.lower()}_data.csv"
    print(download_link)

    first_name = request.user.first_name or "BI Admin"
    email = recipient_email

    message = (
        f"Dear {first_name}, <br><br>Kindly click on the below link to download your requested report. <br>"
        f"<p/>Click the button below to download the file <p/>"
        f"<div style='text-align:left'><a href='{download_link}' target='_blank' "
        f"style='background-color: #67C1F0; color: white; padding: 15px 25px; text-align: center; "
        f"text-decoration: none; display: inline-block;'>Download</a></div><br>"
    )
    subject = "Report Download"
    contents = render(
        None, "default_template.html", context={"message": message}
    ).content.decode("utf-8")
    send_email(contents, email, subject)
    return "Email sent successfully."
