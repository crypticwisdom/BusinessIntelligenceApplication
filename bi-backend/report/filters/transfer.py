from datacore.modules.utils import log_request
from datetime import datetime, timedelta
from ..models import PayarenaExchange, UpConTerminalConfig, UpDms, SettlementDetail
from account.models import Institution
from django.forms.models import model_to_dict
import json
from django.db import connections


def transfer_issue_filter(data):

    source_bank = data.get("issuer_institution", None)
    destination_bank = data.get("acquirer_institution", None)
    value = data.get("amount", None)
    report_type = data.get("report", "issuer")
    chargeback = data.get("chargeback", None)
    statusCode = data.get("status", None)
    requestTime = data.get("start_date", None)
    responseTime = data.get("end_date", None)
    volume = data.get("volume", None)
    settlement = data.get("settlement", None)

    sql_query = "SELECT * FROM payarena_exchange"
    query_params = []
    where_clauses = []

    if source_bank:
        code = Institution.objects.filter(name=source_bank).first()
        if code:
            where_clauses.append("source_bank = %s")
            query_params.append(code.exchangeCode)

    if destination_bank:
        code = Institution.objects.filter(name=destination_bank).first()
        if code:
            where_clauses.append("dest_bank = %s")
            query_params.append(code.exchangeCode)

    if value:
        operator_mapping = {
            ">=": ">=",
            ">": ">",
            "<=": "<=",
            "<": "<",
            "=": "=",
        }

        operator = ""
        amount_value = value

        for op, lookup in operator_mapping.items():
            if value.endswith(op):
                operator = lookup
                amount_value = value[: -len(op)].rstrip()
                break

        if operator:
            where_clauses.append(f"amount {operator} %s")
            query_params.append(amount_value)

    if statusCode:
        if statusCode == "approved":
            where_clauses.append("status_code = '00'")
        elif statusCode == "declined":
            where_clauses.append("status_code != '00'")

    if requestTime and responseTime:
        requestTime = datetime.strptime(requestTime, "%Y-%m-%d")
        responseTime = datetime.strptime(responseTime, "%Y-%m-%d") + timedelta(days=1)
        where_clauses.append("request_time BETWEEN %s AND %s")
        query_params.append(requestTime)
        query_params.append(responseTime)

    if where_clauses:
        sql_query += " WHERE " + " AND ".join(where_clauses)

    if volume:
        sql_query += " LIMIT %s"
        query_params.append(int(volume))

    print(sql_query, query_params)

    with connections["etl_db"].cursor() as cursor:
        cursor.execute(sql_query, query_params)
        transfer_entries = cursor.fetchall()

    if chargeback and chargeback.lower() == "yes":
        rrn_list = [row[0] for row in transfer_entries]
        sql_query = "SELECT trans_id FROM up_dms WHERE trans_id IN %s"
        query_params = [tuple(rrn_list)]

        with connections["etl_db"].cursor() as cursor:
            cursor.execute(sql_query, query_params)
            chargeback_rrns = [row[0] for row in cursor.fetchall()]

        transfer_entries = [
            row for row in transfer_entries if row[0] in chargeback_rrns
        ]

    if settlement and settlement.lower() == "yes":
        request_id_list = [row[0] for row in transfer_entries]
        sql_query = "SELECT trannumber FROM settlement_detail WHERE trannumber IN %s"
        query_params = [tuple(request_id_list)]

        with connections["etl_db"].cursor() as cursor:
            cursor.execute(sql_query, query_params)
            settlement_rrns = [row[0] for row in cursor.fetchall()]

        transfer_entries = [
            row for row in transfer_entries if row[0] in settlement_rrns
        ]

    return transfer_entries


# def transfer_issue_filter(data):

#     source_bank: str = data.get("issuer_institution", None)
#     destination_bank: str = data.get("acquirer_institution", None)
#     value: str = data.get("amount", None)
#     report_type: str = data.get("report", "issuer")
#     chargeback: str = data.get("chargeback", None)
#     statusCode: str = data.get("status", None)
#     requestTime: datetime = data.get("start_date", None)
#     responseTime: datetime = data.get("end_date", None)
#     volume: int = data.get("volume", None)
#     settlement: str = data.get("settlement", None)

#     # filter all data
#     # print(source_bank,destination_bank)
#     transfer_entries: PayarenaExchange = PayarenaExchange.objects.using("etl_db").all()
#     if source_bank:
#         # if report_type == "issuer":
#         #     code=user.institution.exchangeCode
#         #     transfer_entries.filter(source_bank=code)
#         # else:
#         code = Institution.objects.filter(name=source_bank)
#         transfer_entries = transfer_entries.filter(source_bank=code[0].exchangeCode)
#     if destination_bank:
#         # if report_type == "issuer":
#         code = Institution.objects.filter(name=destination_bank)
#         transfer_entries = transfer_entries.filter(dest_bank=code[0].exchangeCode)
#         # else:
#         #     code=user.institution.exchangeCode
#         #     transfer_entries.filter(dest_bank=code)
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
#             transfer_entries = transfer_entries.filter(**{lookup_field: amount_value})

#     if statusCode:
#         if statusCode == "approved":
#             transfer_entries = transfer_entries.filter(status_code="00")
#         elif statusCode == "declined":
#             transfer_entries = transfer_entries.exclude(status_code="00")

#     if requestTime and responseTime:
#         requestTime = datetime.strptime(requestTime, "%Y-%m-%d")

#         responseTime = datetime.strptime(responseTime, "%Y-%m-%d")

#         # Increase the end_date by one day
#         responseTime = responseTime + timedelta(days=1)
#         transfer_entries = transfer_entries.filter(
#             request_time__range=(requestTime, responseTime)
#         )
#     if volume:
#         transfer_entries = transfer_entries[: int(volume)]

#     if chargeback:
#         if chargeback.lower() == "yes":
#             values_to_check = transfer_entries.values_list("rrn", flat=True)

#             queryset_updms = UpDms.objects.using("etl_db").filter(
#                 trans_id__in=values_to_check
#             )

#             transfer_entries = transfer_entries.filter(
#                 rrn__in=queryset_updms.values_list("trans_id", flat=True)
#             )
#     if settlement:
#         if settlement.lower() == "yes":
#             values_to_check = transfer_entries.values_list("request_id", flat=True)

#             queryset_updms = SettlementDetail.objects.using("etl_db").filter(
#                 trannumber__in=values_to_check
#             )

#             transfer_entries = transfer_entries.filter(
#                 rrn__in=queryset_updms.values_list("trannumber", flat=True)
#             )
#         #     return result_json
#     return transfer_entries
