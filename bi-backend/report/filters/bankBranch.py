from report.models import BankBranchEtl, UpDms, SettlementDetail
from account.models import Institution
from datacore.modules.utils import log_request
from django.db import connections
from datetime import datetime, timedelta


def bankBranch(data: dict) -> tuple:
    try:
        # Extract parameters
        issuer_institu = data.get("issuer")
        value = data.get("value")
        charged_back = data.get("chargeBack")
        settlement = data.get("settlement")
        status = data.get("transactionStatus")
        start_date = data.get("startDate")
        end_date = data.get("endDate")
        volume = data.get("volume")

        # Initialize SQL query
        sql_query = "SELECT * FROM bank_branch_etl WHERE 1=1"
        query_params = []

        # Filter by issuer institution
        if issuer_institu:
            institution = Institution.objects.filter(name=issuer_institu).first()
            if institution:
                sql_query += " AND bank_code = %s"
                query_params.append(institution.bespokeCode)

        # Filter by transaction amount
        if value:
            operator_mapping = {
                ">=": "__gte",
                ">": "__gt",
                "<=": "__lte",
                "<": "__lt",
                "=": "",
            }
            operator = ""
            amount_value = value.strip()

            for op, lookup in operator_mapping.items():
                if amount_value.endswith(op):
                    operator = lookup
                    amount_value = amount_value[: -len(op)].strip()
                    break

            if operator:
                sql_query += f" AND tran_amount {operator} %s"
                query_params.append(amount_value)

        # Get rrn values from BankBranchEtl
        values_to_check = list(
            BankBranchEtl.objects.using("etl_db").values_list("rrn", flat=True)
        )

        # Filter by charge back status
        if charged_back and charged_back.lower() == "yes" and values_to_check:
            placeholders = ", ".join(["%s"] * len(values_to_check))
            sql_query += f" AND rrn IN (SELECT trans_id FROM up_dms WHERE trans_id IN ({placeholders}))"
            query_params.extend(values_to_check)

        # Filter by settlement status
        if settlement and settlement.lower() == "yes" and values_to_check:
            placeholders = ", ".join(["%s"] * len(values_to_check))
            sql_query += f" AND rrn IN (SELECT trannumber FROM settlement_detail WHERE trannumber IN ({placeholders}))"
            query_params.extend(values_to_check)

        # Filter by transaction status
        if status:
            response_code = "00" if status == "approved" else "00"
            sql_query += " AND response_code %s %s"
            query_params.append("=" if status == "approved" else "!=")
            query_params.append(response_code)

        # Filter by date range
        if start_date and end_date:
            end_date = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
            sql_query += " AND tran_data BETWEEN %s AND %s"
            query_params.append(start_date)
            query_params.append(end_date)

        # Apply volume limit
        if volume:
            sql_query += " LIMIT %s"
            query_params.append(int(volume))

        print(sql_query, query_params)

        # Execute the SQL query
        with connections["etl_db"].cursor() as cursor:
            cursor.execute(sql_query, query_params)
            all_branch_data = cursor.fetchall()

        return True, all_branch_data

    except Exception as e:
        log_request(f"Unable to filter due to incorrect parameters: {e}")
        return False, "Invalid parameters sent"


# from ..models import BankBranchEtl, UpDms, SettlementDetail
# from account.models import Institution,Channel,ExtraParameters,TransactionType
# from datacore.modules.utils import log_request
# from django.db.models import Q

# from datetime import datetime ,timedelta
# import ast


# def bankBranch(data: dict):
#     # print("data woring")
#     issuer_institu: str = data.get("issuer", None)
#     acquirer_institu: str = data.get("acquirer", None)
#     value: str = data.get("value", None)
#     domestic: str = data.get("domestic", None)
#     charged_back: str = data.get("chargeBack", None)
#     settlement: str = data.get("settlement", None)
#     status: str = data.get("transactionStatus", None)
#     start_date: datetime = data.get("startDate", None)
#     end_date: datetime = data.get("endDate", None)
#     volume: str = data.get("volume", None)

#     bank_branch: BankBranchEtl = BankBranchEtl.objects.using("etl_db").all()
#     if issuer_institu:
#         code = Institution.objects.filter(name=issuer_institu)
#         bank_branch.filter(bank_code=code[0].bespokeCode)

#     if value:

#         operator_mapping = {
#             '>=': '__gte',
#             '>': '__gt',
#             '<=': '__lte',
#             '<': '__lt',
#             '=': '',
#         }

#         # Initialize operator and amount_value
#         operator = ''
#         amount_value = value

#         # Iterate over operator_mapping to find the matching operator
#         for op, lookup in operator_mapping.items():
#             if value.endswith(op):
#                 operator = lookup
#                 amount_value = value[:-len(op)].rstrip()  # Remove the operator and any trailing whitespace
#                 break

#         if operator:
#             lookup_field = f'tran_amount{operator}'
#             bank_branch = bank_branch.filter(**{lookup_field: amount_value})

#     if charged_back:
#         if charged_back.lower() == "yes":
#             values_to_check = bank_branch.values_list('rrn', flat=True)

#             queryset_updms = UpDms.objects.using("etl_db").filter(trans_id__in=values_to_check)

#             bank_branch = bank_branch.filter(rrn__in=queryset_updms.values_list('trans_id', flat=True))

#     if settlement:
#         if settlement.lower() == "yes":
#             values_to_check = bank_branch.values_list('rrn', flat=True)

#             queryset_updms = SettlementDetail.objects.using("etl_db").filter(trannumber__in=values_to_check)

#             bank_branch = bank_branch.filter(rrn__in=queryset_updms.values_list('trannumber', flat=True))
#     if status:
#         if status == "approved":
#             bank_branch.filter(response_code="00")
#         elif status == "declined":
#             bank_branch.exclude(response_code="00")
#     if start_date and end_date:
#         end_date = datetime.strptime(end_date, "%Y-%m-%d")

#         # Increase the end_date by one day
#         end_date = end_date + timedelta(days=1)
#         bank_branch.filter(tran_data__range=(start_date, end_date))
#     if volume:
#         bank_branch = bank_branch[:int(volume)]

#     return bank_branch
