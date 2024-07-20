from ..models import BankBranchEtl, UpDms, UpConTerminalConfig, SettlementDetail
from account.models import Institution, Channel, ExtraParameters, TransactionType
from datacore.modules.utils import log_request
from django.db import connections
from django.db.models import Q

from datetime import datetime, timedelta
import ast


def terminal(data: dict):
    source = data.get("source")
    choice = data.get("choice")
    destination = data.get("destination")
    value = data.get("value")
    terminal = data.get("terminal")
    merchant = data.get("merchant")
    appName = data.get("appName")
    requestTime = data.get("requestTime")
    volume = data.get("volume")
    responseTime = data.get("responseTime")
    chargeback = data.get("chargeback")
    settlement = data.get("settlement")
    transactionStatus = data.get("transactionStatus")

    if choice == "terminal_configuration":
        sql_query = "SELECT * FROM up_con_terminal_config WHERE schema_or_table_name = 'terminal_configuration'"
        query_params = []

        if source:
            code = Institution.objects.filter(name=source).first()
            if code:
                sql_query += " AND bank_code = %s"
                query_params.append(code.bespokeCode)

        if terminal:
            sql_query += " AND tid = %s"
            query_params.append(terminal)

        if merchant:
            sql_query += " AND mid = %s"
            query_params.append(merchant)

        if appName:
            sql_query += " AND app_name = %s"
            query_params.append(appName)

    else:
        sql_query = "SELECT * FROM up_con_terminal_config WHERE schema_or_table_name = 'terminal_message'"
        query_params = []

    if source:
        sql_query += " AND source = %s"
        query_params.append(source)

    if destination:
        sql_query += " AND destination = %s"
        query_params.append(destination)

    if value:
        operator_mapping = {
            ">=": "__gte",
            ">": "__gt",
            "<=": "__lte",
            "<": "__lt",
            "=": "",
        }

        operator = ""
        amount_value = value

        for op, lookup in operator_mapping.items():
            if value.endswith(op):
                operator = lookup
                amount_value = value[: -len(op)].rstrip()
                break

        if operator:
            lookup_field = f"amount{operator}"
            sql_query += f" AND {lookup_field} = %s"
            query_params.append(amount_value)

    if transactionStatus:
        if transactionStatus == "approved":
            sql_query += " AND resp_code = '00'"
        elif transactionStatus == "declined":
            sql_query += " AND resp_code != '00'"

    if requestTime and responseTime:
        requestTime = datetime.strptime(requestTime, "%Y-%m-%d")
        responseTime = datetime.strptime(responseTime, "%Y-%m-%d") + timedelta(days=1)
        sql_query += " AND message_date BETWEEN %s AND %s"
        query_params.append(requestTime)
        query_params.append(responseTime)

    if volume:
        sql_query += " LIMIT %s"
        query_params.append(int(volume))

    with connections["etl_db"].cursor() as cursor:
        cursor.execute(sql_query, query_params)
        channel = cursor.fetchall()

    if chargeback and chargeback.lower() == "yes":
        rrn_list = [row[0] for row in channel]
        sql_query = "SELECT trans_id FROM up_dms WHERE trans_id IN %s"
        query_params = [tuple(rrn_list)]

        with connections["etl_db"].cursor() as cursor:
            cursor.execute(sql_query, query_params)
            chargeback_rrns = [row[0] for row in cursor.fetchall()]

        channel = [row for row in channel if row[0] in chargeback_rrns]

    if settlement and settlement.lower() == "yes":
        rrn_list = [row[0] for row in channel]
        sql_query = "SELECT trannumber FROM settlement_detail WHERE trannumber IN %s"
        query_params = [tuple(rrn_list)]

        with connections["etl_db"].cursor() as cursor:
            cursor.execute(sql_query, query_params)
            settlement_rrns = [row[0] for row in cursor.fetchall()]

        channel = [row for row in channel if row[0] in settlement_rrns]

    return channel


# def terminal(data: dict):
#     source: str = data.get("source", None)
#     choice: str = data.get("choice", None)
#     destination: str = data.get("destination", None)
#     value: str = data.get("value", None)
#     terminal: str = data.get("terminal", None)
#     merchant: str = data.get("merchant", None)
#     appName: str = data.get("appName", None)
#     requestTime: str = data.get("requestTime", None)
#     volume: str = data.get("volume", None)
#     responseTime: str = data.get("responseTime", None)
#     chargeback: str = data.get("chargeback", None)
#     settlement: str = data.get("settlement", None)
#     transactionStatus: str = data.get("transactionStatus", None)
#     if choice == "terminal_configuration":
#         channel: UpConTerminalConfig = UpConTerminalConfig.objects.using(
#             "etl_db"
#         ).filter(schema_or_table_name="terminal_configuration")
#         if source:
#             code = Institution.objects.filter(name=source)
#             channel = channel.filter(bank_code=code[0].bespokeCode)
#         if terminal:
#             channel = channel.filter(tid=terminal)
#         if merchant:
#             channel = channel.filter(mid=merchant)
#         if appName:
#             channel = channel.filter(app_name=appName)
#         return channel

#     else:
#         channel: UpConTerminalConfig = UpConTerminalConfig.objects.using(
#             "etl_db"
#         ).filter(schema_or_table_name="terminal_message")

#     if source:
#         channel = channel.filter(source=source)
#     if destination:
#         channel = channel.filter(destination=destination)
#     if value:

#         operator_mapping = {
#             ">=": "__gte",
#             ">": "__gt",
#             "<=": "__lte",
#             "<": "__lt",
#             "=": "",
#         }

#         # Initialize operator and amount_value
#         operator = ""
#         amount_value = value

#         # Iterate over operator_mapping to find the matching operator
#         for op, lookup in operator_mapping.items():
#             if value.endswith(op):
#                 operator = lookup
#                 amount_value = value[
#                     : -len(op)
#                 ].rstrip()  # Remove the operator and any trailing whitespace
#                 break

#         if operator:
#             lookup_field = f"amount{operator}"
#             channel = channel.filter(**{lookup_field: amount_value})
#     if transactionStatus:
#         if transactionStatus == "approved":
#             channel = channel.filter(resp_code="00")
#         elif transactionStatus == "declined":
#             channel = channel.exclude(resp_code="00")

#     if requestTime:
#         requestTime = datetime.strptime(requestTime, "%Y-%m-%d")
#         responseTime = datetime.strptime(responseTime, "%Y-%m-%d")

#         # Increase the end_date by one day
#         responseTime = responseTime + timedelta(days=1)
#         channel = channel.filter(message_date__range=(requestTime, responseTime))
#     if volume:
#         channel = channel[: int(volume)]

#     if chargeback:
#         if chargeback.lower() == "yes":
#             values_to_check = channel.values_list("rrn", flat=True)

#             queryset_updms = UpDms.objects.using("etl_db").filter(
#                 trans_id__in=values_to_check
#             )

#             channel = channel.filter(
#                 rrn__in=queryset_updms.values_list("trans_id", flat=True)
#             )
#     if settlement:
#         if settlement.lower() == "yes":
#             values_to_check = channel.values_list("rrn", flat=True)

#             queryset_updms = SettlementDetail.objects.using("etl_db").filter(
#                 trannumber__in=values_to_check
#             )

#             channel = channel.filter(
#                 rrn__in=queryset_updms.values_list("trannumber", flat=True)
#             )

#     return channel
