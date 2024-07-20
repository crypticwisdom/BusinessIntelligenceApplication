from report.models import (
    CardAccountDetailsIssuing,
    Holdertags,
    CardClientPersonnelIssuing,
    Transactions,
)
from account.models import Institution
from datacore.modules.utils import log_request
from django.db import connections
import ast


def card(data: dict):
    report_type = data.get("type", None)
    if report_type == "number-of-card":
        return "card", number_of_card(data)
    elif report_type == "activate-card":
        return "card", activate_card(data)
    elif report_type == "expire-card":
        return "card", expire_card(data)
    elif report_type == "valid-card":
        return "card", valid_card(data)
    elif report_type == "block_card":
        return "card", block_card(data)
    elif report_type == "payattitude":
        return "holdertag", number_of_payattitude(data)
    elif report_type == "contact":
        return "transaction", contact_and_contactless(data, True)
    elif report_type == "contactless":
        return "transaction", contact_and_contactless(data, False)
    else:
        return "holdertag", disable_subscription(data)


def number_of_card(data: dict):
    institu = data.get("institution")
    created_start = data.get("createdStart")
    created_end = data.get("createdEnd")
    phone = data.get("phone")
    schema = ast.literal_eval(data.get("shemaNAME", "[]"))
    currency = ast.literal_eval(data.get("currency", "[]"))
    pan = data.get("pan")
    status = data.get("status")
    bins = ast.literal_eval(data.get("bin", "[]"))

    sql_query = "SELECT * FROM card_account_details_issuing WHERE 1=1"
    query_params = []

    if institu:
        code = Institution.objects.filter(name=institu).first()
        if code:
            sql_query += " AND branch = %s"
            query_params.append(code.branch)

    if created_start and created_end:
        sql_query += " AND tcard_createdate BETWEEN %s AND %s"
        query_params.append(created_start)
        query_params.append(created_end)

    if schema:
        sql_query += " AND " + " OR ".join(["pan LIKE %s" for _ in schema])
        query_params.extend([f"{value}%" for value in schema])

    if currency:
        sql_query += " AND currency_no IN %s"
        query_params.append(tuple(currency))

    if bins:
        sql_query += " AND " + " OR ".join(["pan LIKE %s" for _ in bins])
        query_params.extend([f"{bin}%" for bin in bins])

    if pan:
        sql_query += " AND pan = %s"
        query_params.append(pan)

    if status:
        sql_query += " AND tcard_sign_stat = %s"
        query_params.append(int(status))

    if phone:
        phone_filter = CardClientPersonnelIssuing.objects.using("etl_db").filter(
            phone=phone
        )
        client_id_list = phone_filter.values_list("client_id", flat=True)
        if client_id_list:
            sql_query += " AND client_id IN %s"
            query_params.append(tuple(client_id_list))

    with connections["etl_db"].cursor() as cursor:
        cursor.execute(sql_query, query_params)
        cards = cursor.fetchall()

    return cards


def activate_card(data: dict):
    institu = data.get("institution")
    created_start = data.get("createdStart")
    created_end = data.get("createdEnd")
    phone = data.get("phone")
    schema = ast.literal_eval(data.get("shemaNAME", "[]"))
    bins = ast.literal_eval(data.get("bins", "[]"))
    cancel = data.get("cancel")
    currency = ast.literal_eval(data.get("currency", "[]"))
    pan = data.get("pan")
    signstat = data.get("signstat")

    sql_query = "SELECT * FROM card_account_details_issuing WHERE 1=1"
    query_params = []

    if institu:
        code = Institution.objects.filter(name=institu).first()
        if code:
            sql_query += " AND branch = %s"
            query_params.append(code.branch)

    if schema:
        sql_query += " AND " + " OR ".join(["pan LIKE %s" for _ in schema])
        query_params.extend([f"{value}%" for value in schema])

    if bins:
        sql_query += " AND " + " OR ".join(["pan LIKE %s" for _ in bins])
        query_params.extend([f"{bin}%" for bin in bins])

    if pan:
        sql_query += " AND pan = %s"
        query_params.append(pan)

    if currency:
        sql_query += " AND currency_no IN %s"
        query_params.append(tuple(currency))

    if created_start and created_end:
        sql_query += " AND tcard_createdate BETWEEN %s AND %s"
        query_params.append(created_start)
        query_params.append(created_end)

    if cancel:
        sql_query += " AND tcard_canceldate >= %s"
        query_params.append(cancel)

    if phone:
        phone_filter = CardClientPersonnelIssuing.objects.using("etl_db").filter(
            phone=phone
        )
        client_id_list = phone_filter.values_list("client_id", flat=True)
        if client_id_list:
            sql_query += " AND client_id IN %s"
            query_params.append(tuple(client_id_list))

    if signstat:
        sql_query += " AND tcard_sign_stat = %s"
        query_params.append(4)

    with connections["etl_db"].cursor() as cursor:
        cursor.execute(sql_query, query_params)
        cards = cursor.fetchall()

    return cards


def expire_card(data: dict):
    institu = data.get("institution")
    created_start = data.get("createdStart")
    created_end = data.get("createdEnd")
    phone = data.get("phone")
    schema = ast.literal_eval(data.get("shemaNAME", "[]"))
    bins = ast.literal_eval(data.get("bins", "[]"))
    cancel = data.get("cancel")
    currency = ast.literal_eval(data.get("currency", "[]"))
    pan = data.get("pan")
    signstat = data.get("signstat")

    sql_query = "SELECT * FROM card_account_details_issuing WHERE 1=1"
    query_params = []

    if institu:
        code = Institution.objects.filter(name=institu).first()
        if code:
            sql_query += " AND branch = %s"
            query_params.append(code.branch)

    if bins:
        sql_query += " AND " + " OR ".join(["pan LIKE %s" for _ in bins])
        query_params.extend([f"{bin}%" for bin in bins])

    if phone:
        phone_filter = CardClientPersonnelIssuing.objects.using("etl_db").filter(
            phone=phone
        )
        client_id_list = phone_filter.values_list("client_id", flat=True)
        if client_id_list:
            sql_query += " AND client_id IN %s"
            query_params.append(tuple(client_id_list))

    if schema:
        sql_query += " AND " + " OR ".join(["pan LIKE %s" for _ in schema])
        query_params.extend([f"{value}%" for value in schema])

    if pan:
        sql_query += " AND pan = %s"
        query_params.append(pan)

    if currency:
        sql_query += " AND currency_no IN %s"
        query_params.append(tuple(currency))

    if created_start and created_end:
        sql_query += " AND tcard_createdate BETWEEN %s AND %s"
        query_params.append(created_start)
        query_params.append(created_end)

    if cancel:
        sql_query += " AND tcard_canceldate >= %s"
        query_params.append(cancel)

    if signstat:
        sql_query += " AND tcard_sign_stat = %s"
        query_params.append(9)

    with connections["etl_db"].cursor() as cursor:
        cursor.execute(sql_query, query_params)
        cards = cursor.fetchall()

    return cards


def valid_card(data: dict):
    institu = data.get("institution")
    bins = ast.literal_eval(data.get("bins", "[]"))
    phone = data.get("phone")
    cancel = data.get("cancel")
    currency = ast.literal_eval(data.get("currency", "[]"))
    pan = data.get("pan")
    signstat = data.get("signstat")

    sql_query = "SELECT * FROM card_account_details_issuing WHERE 1=1"
    query_params = []

    if institu:
        code = Institution.objects.filter(name=institu).first()
        if code:
            sql_query += " AND branch = %s"
            query_params.append(code.branch)

    if bins:
        sql_query += " AND " + " OR ".join(["pan LIKE %s" for _ in bins])
        query_params.extend([f"{bin}%" for bin in bins])

    if pan:
        sql_query += " AND pan = %s"
        query_params.append(pan)

    if currency:
        sql_query += " AND currency_no IN %s"
        query_params.append(tuple(currency))

    if cancel:
        sql_query += " AND tcard_canceldate >= %s"
        query_params.append(cancel)

    if signstat:
        sql_query += " AND tcard_sign_stat IN %s"
        query_params.append(tuple([1, 2, 3, 4, 11, 12, 13]))

    if phone:
        phone_filter = CardClientPersonnelIssuing.objects.using("etl_db").filter(
            phone=phone
        )
        client_id_list = phone_filter.values_list("client_id", flat=True)
        if client_id_list:
            sql_query += " AND client_id IN %s"
            query_params.append(tuple(client_id_list))

    with connections["etl_db"].cursor() as cursor:
        cursor.execute(sql_query, query_params)
        cards = cursor.fetchall()

    return cards


def block_card(data: dict):
    institu = data.get("institution")
    bins = ast.literal_eval(data.get("bins", "[]"))
    cancel = data.get("cancel")
    phone = data.get("phone")
    currency = ast.literal_eval(data.get("currency", "[]"))
    pan = data.get("pan")
    signstat = data.get("signstat")

    sql_query = "SELECT * FROM card_account_details_issuing WHERE 1=1"
    query_params = []

    if institu:
        code = Institution.objects.filter(name=institu).first()
        if code:
            sql_query += " AND branch = %s"
            query_params.append(code.branch)

    if bins:
        sql_query += " AND " + " OR ".join(["pan LIKE %s" for _ in bins])
        query_params.extend([f"{bin}%" for bin in bins])

    if pan:
        sql_query += " AND pan = %s"
        query_params.append(pan)

    if currency:
        sql_query += " AND currency_no IN %s"
        query_params.append(tuple(currency))

    if signstat:
        sql_query += " AND tcard_sign_stat = %s"
        query_params.append(18)

    if phone:
        phone_filter = CardClientPersonnelIssuing.objects.using("etl_db").filter(
            phone=phone
        )
        client_id_list = phone_filter.values_list("client_id", flat=True)
        if client_id_list:
            sql_query += " AND client_id IN %s"
            query_params.append(tuple(client_id_list))

    with connections["etl_db"].cursor() as cursor:
        cursor.execute(sql_query, query_params)
        cards = cursor.fetchall()

    return cards


def disable_subscription(data: dict):
    institu = data.get("institution")
    status = ast.literal_eval(data.get("status", "[]"))
    phone = data.get("phone")

    sql_query = "SELECT * FROM holdertags WHERE 1=1"
    query_params = []

    if institu:
        code = Institution.objects.filter(name=institu).first()
        if code:
            sql_query += " AND institution = %s"
            query_params.append(code.bespokeCode)

    if phone:
        sql_query += " AND phone ILIKE %s"
        query_params.append(phone)

    if status:
        sql_query += " AND status IN %s"
        query_params.append(tuple(status))

    with connections["etl_db"].cursor() as cursor:
        cursor.execute(sql_query, query_params)
        tags = cursor.fetchall()

    return tags


def number_of_payattitude(data: dict):
    created = data.get("created")
    institu = data.get("institution")

    sql_query = "SELECT * FROM holdertags WHERE 1=1"
    query_params = []

    if institu:
        code = Institution.objects.filter(name=institu).first()
        if code:
            sql_query += " AND institution = %s"
            query_params.append(code.bespokeCode)

    if created:
        sql_query += " AND created >= %s"
        query_params.append(created)

    with connections["etl_db"].cursor() as cursor:
        cursor.execute(sql_query, query_params)
        holders = cursor.fetchall()

    return holders


def contact_and_contactless(data: dict, difftype):
    institu = data.get("institution")
    created_start = data.get("createdStart")
    created_end = data.get("createdEnd")
    schema = ast.literal_eval(data.get("shemaNAME", "[]"))
    bins = ast.literal_eval(data.get("bins", "[]"))
    currency = ast.literal_eval(data.get("currency", "[]"))
    pan = data.get("pan")
    signstat = data.get("signstat")

    sql_query = "SELECT * FROM transactions WHERE department = 'processing' AND 1=1"
    query_params = []

    if institu:
        code = Institution.objects.filter(name=institu).first()
        if code:
            sql_query += " AND issuer_institution_name = %s"
            query_params.append(code.tlaCode)

    if schema:
        sql_query += " AND " + " OR ".join(["pan LIKE %s" for _ in schema])
        query_params.extend([f"{value}%" for value in schema])

    if bins:
        sql_query += " AND " + " OR ".join(["pan LIKE %s" for _ in bins])
        query_params.extend([f"{bin}%" for bin in bins])

    if pan:
        sql_query += " AND pan = %s"
        query_params.append(pan)

    if currency:
        sql_query += " AND account_curreny IN %s"
        query_params.append(tuple(currency))

    if created_start and created_end:
        sql_query += " AND transaction_time BETWEEN %s AND %s"
        query_params.append(created_start)
        query_params.append(created_end)

    if difftype:
        sql_query += " AND pos_entry_mode = '91'"
    else:
        sql_query += " AND pos_entry_mode = '7'"

    with connections["etl_db"].cursor() as cursor:
        cursor.execute(sql_query, query_params)
        cards = cursor.fetchall()

    return cards
