from dateutil.relativedelta import relativedelta
from django.core.cache import cache
from django.db.models import Count, Sum, Q
from elasticsearch import helpers
from django.db import connections
from django.db import connection

from django.utils import timezone
from django.conf import settings
from account.models import UserDetail, Institution
from datacore.modules.utils import (
    get_previous_date,
    get_year_start_and_end_datetime,
    get_month_start_and_end_datetime,
    get_week_start_and_end_datetime,
    get_day_start_and_end_datetime,
    calculate_percentage_change,
)
from report.models import (
    Transactions,
    CardAccountDetailsIssuing,
    UpConTerminalConfig,
    SettlementMeb,
    SettlementDetail,
)
import psycopg2
from datetime import datetime


cache_timeout = settings.CACHE_TIMEOUT
# conn = psycopg2.connect(
#     dbname="your_db", user="your_user", password="your_password", host="your_host"
# )


def transaction_status_per_channel(user, inst_id, channel, duration):
    current_date = timezone.now()

    duration = duration or "yesterday"

    # Fetch user profile and institution details
    with connections["default"].cursor() as cursor:
        cursor.execute(
            "SELECT institution_id FROM account_userdetail WHERE user_id = %s",
            [user.id],
        )
        profile = cursor.fetchone()

    if profile and profile[0]:
        inst_id = profile[0]

    if inst_id:
        with connections["default"].cursor() as cursor:
            cursor.execute(
                "SELECT name, bespoke_code, tla_code FROM account_institution WHERE id = %s",
                [inst_id],
            )
            inst = cursor.fetchone()
    else:
        inst = None

    # Construct cache key
    cache_key = f"{inst}_transaction_status_per_channel_{channel}_{duration}"

    result = cache.get(cache_key)
    if result:
        return result

    # Channel queries
    channel_queries = {
        "pos": ("AND channel ILIKE 'POS'", "AND channel ILIKE '2.0'"),
        "ussd": ("AND channel ILIKE 'USSD'", "AND channel ILIKE '00'"),
        "atm": ("AND channel ILIKE 'ATM'", "AND channel ILIKE '1.0'"),
        "web": (
            "AND channel ILIKE 'Web'",
            "AND channel ILIKE '2.0' AND secondary_channel IN ('8', '81', '82', '83', '84', '85', '86', '87', '88', '89', '61', '62', '52', '59')",
        ),
        "agency": ("AND channel ILIKE 'agency'", "AND channel ILIKE '00'"),
        "bankussd": ("AND channel ILIKE 'BankUSSD'", "AND channel ILIKE '00'"),
        "mobileapp": ("AND channel ILIKE 'Mobile'", "AND channel ILIKE '00'"),
    }
    bespoke_query, tla_query = channel_queries.get(channel, ("", ""))

    if inst:
        bespoke_query += f" AND issuer_institution_name = '{inst[1]}'"
        tla_query += f" AND issuer_institution_name = '{inst[2]}'"

    def fetch_transaction_data(date_range):
        bespoke_sql = f"""
            SELECT 
                COUNT(*) AS total_count,
                COALESCE(SUM(total_amount), 0) AS total_amount,
                COALESCE(SUM(CASE WHEN status_code = '00' THEN total_amount ELSE 0 END), 0) AS success_amount,
                COALESCE(SUM(CASE WHEN status_code = '00' THEN 1 ELSE 0 END), 0) AS success_count,
                COALESCE(SUM(CASE WHEN status_code = '03' THEN total_amount ELSE 0 END), 0) AS failed_amount,
                COALESCE(SUM(CASE WHEN status_code = '03' THEN 1 ELSE 0 END), 0) AS failed_count,
                COALESCE(SUM(CASE WHEN status_code = '05' THEN total_amount ELSE 0 END), 0) AS declined_amount,
                COALESCE(SUM(CASE WHEN status_code = '05' THEN 1 ELSE 0 END), 0) AS declined_count
            FROM transactions
            WHERE transaction_time BETWEEN %s AND %s {bespoke_query} AND department = 'bespoke'
        """
        tla_sql = f"""
            SELECT 
                COUNT(*) AS total_count,
                COALESCE(SUM(amount), 0) AS total_amount,
                COALESCE(SUM(CASE WHEN transaction_status = '1' AND department = 'processing' THEN amount ELSE 0 END), 0) AS success_amount,
                COALESCE(SUM(CASE WHEN department = 'processing' AND transaction_status != '1' THEN amount ELSE 0 END), 0) AS failed_amount,
                COALESCE(SUM(CASE WHEN transaction_status = '1' AND department = 'processing' THEN 1 ELSE 0 END), 0) AS success_count,
                COALESCE(SUM(CASE WHEN department = 'processing' AND transaction_status != '1' THEN 1 ELSE 0 END), 0) AS failed_count
            FROM transactions
            WHERE transaction_time BETWEEN %s AND %s {tla_query} AND department = 'processing'
        """

        with connections["etl_db"].cursor() as cursor:
            cursor.execute(bespoke_sql, date_range)
            bespoke_data = cursor.fetchone()
            cursor.execute(tla_sql, date_range)
            tla_data = cursor.fetchone()
        print(tla_data[0], tla_data[1], "pppppppppp")
        print("======.>>>>")
        print(bespoke_data[1])
        return {
            "total_count": bespoke_data[0] + tla_data[0],
            "total_amount": bespoke_data[1] + tla_data[1],
            "success_amount": bespoke_data[2] + tla_data[2],
            "success_count": bespoke_data[3] + tla_data[4],
            "failed_amount": bespoke_data[4] + tla_data[3],
            "failed_count": bespoke_data[5] + tla_data[5],
            "declined_amount": bespoke_data[6],
            "declined_count": bespoke_data[7],
        }

    def get_date_range(delta, duration):
        if duration == "thisMonth":
            start = (current_date - relativedelta(months=delta)).replace(day=1)
            end = (start + relativedelta(months=1)).replace(day=1) - relativedelta(
                days=1
            )
        elif duration == "thisWeek":
            start = (current_date - relativedelta(weeks=delta)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            start -= relativedelta(days=start.weekday())
            end = start + relativedelta(days=6)
        elif duration == "thisYear":
            start = (current_date - relativedelta(years=delta)).replace(month=1, day=1)
            end = start.replace(month=12, day=31)
        else:
            start = (current_date - relativedelta(days=delta)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            end = start + relativedelta(days=1) - relativedelta(microseconds=1)
        return (start, end)

    yesterday, weekly, monthly, yearly = [], [], [], []
    for delta in range(6, -1, -1):
        date_range = get_date_range(delta, duration)
        transaction_data = fetch_transaction_data(date_range)

        entry = {
            "transactionAmount": transaction_data["total_amount"],
            "totalCount": transaction_data["total_count"],
            "totalSuccessAmount": transaction_data["success_amount"],
            "totalSuccessCount": transaction_data["success_count"],
            "totalFailedAmount": transaction_data["failed_amount"],
            "totalFailedCount": transaction_data["failed_count"],
            "totalDeclinedAmount": transaction_data["declined_amount"],
            "totalDeclinedCount": transaction_data["declined_count"],
        }

        if duration == "thisMonth":
            entry["month"] = date_range[0].strftime("%b")
            monthly.append(entry)
        elif duration == "thisWeek":
            entry["week"] = f"week {delta}"
            weekly.append(entry)
        elif duration == "thisYear":
            entry["year"] = date_range[0].strftime("%Y")
            yearly.append(entry)
        else:
            entry["day"] = date_range[0].strftime("%d %b")
            yesterday.append(entry)

    result = {
        "daily": yesterday,
        "weekly": weekly,
        "monthly": monthly,
        "yearly": yearly,
    }
    cache.set(cache_key, result, timeout=cache_timeout)

    return result


def total_admin_count_report(user, inst_id):
    profile = UserDetail.objects.get(user=user)

    if profile.institution:
        inst = profile.institution
    elif inst_id is not None:
        inst = Institution.objects.get(id=inst_id)
    else:
        inst = None

    if inst is not None:
        cache_key = f"{str(inst.name).replace(' ', '')}_admin_count"
    else:
        cache_key = "all_admin_count"

    result = cache.get(cache_key)
    if result is None:
        if inst is None:
            user_counts = (
                UserDetail.objects.all().values("role").annotate(count=Count("role"))
            )
        else:
            user_counts = (
                UserDetail.objects.filter(institution=inst)
                .values("role")
                .annotate(count=Count("role"))
            )
        result = {role["role"]: role["count"] for role in user_counts}
        cache.set(key=cache_key, value=result, timeout=cache_timeout)
        connections["etl_db"].close()

    return result


def total_transaction_count_and_value(user, inst_id, duration):
    user_profile = UserDetail.objects.get(user=user)
    today = timezone.now()

    if duration == "" or duration is None:
        duration = "yesterday"

    if duration == "thisMonth":
        start_date, end_date = get_month_start_and_end_datetime(today)
    elif duration == "thisWeek":
        start_date, end_date = get_week_start_and_end_datetime(today)
    elif duration == "thisYear":
        start_date, end_date = get_year_start_and_end_datetime(today)
    else:
        yesterday = get_previous_date(today, 1)
        start_date, end_date = get_day_start_and_end_datetime(yesterday)

    if user_profile.institution:
        inst = user_profile.institution
    elif inst_id:
        inst = Institution.objects.get(id=inst_id)
    else:
        inst = None

    if inst is not None:
        cache_key = (
            f"{str(inst.name).replace(' ', '')}_transaction_count_and_value_{duration}"
        )
    else:
        cache_key = f"all_transaction_count_and_value_{duration}"

    result = cache.get(cache_key)
    if result is None:
        with connections["etl_db"].cursor() as cursor:
            cursor.execute(
                """
                SELECT 
                    COUNT(*) AS trans_count,
                    COALESCE(SUM(CASE WHEN department = 'bespoke' THEN total_amount ELSE 0 END), 0) AS trans_tla_amount,
                    COALESCE(SUM(CASE WHEN department = 'processing' THEN amount ELSE 0 END), 0) AS tran_bespoke_amount,
                    COALESCE(SUM(CASE WHEN department = 'bespoke' THEN total_amount ELSE 0 END), 0) AS overall_tla_amount,
                    COALESCE(SUM(CASE WHEN department = 'processing' THEN amount ELSE 0 END), 0) AS overall_bespoke_amount
                FROM transactions
                WHERE transaction_time BETWEEN %s AND %s
            """,
                [start_date, end_date],
            )
            (
                trans_count,
                trans_tla_amount,
                tran_bespoke_amount,
                overall_tla_amount,
                overall_bespoke_amount,
            ) = cursor.fetchone()

        if inst is None:
            result = {
                "count": trans_count,
                "amount": trans_tla_amount + tran_bespoke_amount,
            }
        else:
            with connections["etl_db"].cursor() as cursor:
                cursor.execute(
                    """
                    SELECT 
                        COUNT(*) AS bespoke_count,
                        COALESCE(SUM(total_amount), 0) AS bespoke_amount
                    FROM transactions
                    WHERE issuer_institution_name = %s
                    AND transaction_time BETWEEN %s AND %s
                """,
                    [inst.bespokeCode, start_date, end_date],
                )
                bespoke_count, bespoke_amount = cursor.fetchone()

                cursor.execute(
                    """
                    SELECT 
                        COUNT(*) AS tla_count,
                        COALESCE(SUM(amount), 0) AS tla_amount
                    FROM transactions
                    WHERE issuer_institution_name = %s
                    AND transaction_time BETWEEN %s AND %s
                """,
                    [inst.tlaCode, start_date, end_date],
                )
                tla_count, tla_amount = cursor.fetchone()

            result = {
                "count": bespoke_count + tla_count,
                "amount": bespoke_amount + tla_amount,
            }

        result.update(
            {
                "overallTotalTransactionAmount": overall_tla_amount
                + overall_bespoke_amount
            }
        )
        cache.set(key=cache_key, value=result, timeout=cache_timeout)
        connections["etl_db"].close()

    return result


# def transaction_status(user, inst_id):
#     current_date = timezone.now()
#     profile = UserDetail.objects.get(user=user)
#     yesterday = []
#     monthly = []
#     yearly = []
#     weekly = []

#     if profile.institution:
#         inst = profile.institution
#     elif inst_id is not None:
#         inst = Institution.objects.get(id=inst_id)
#     else:
#         inst = None

#     if inst is not None:
#         cache_key = f"{str(inst.name).replace(' ', '')}_transaction_status_report"
#     else:
#         cache_key = "all_transaction_status_report"

#     if cache.get(cache_key):
#         result = cache.get(cache_key)
#     else:
#         result = dict()
#         week = 0

#         for delta in range(6, -1, -1):
#             yesterday_date = current_date - relativedelta(days=delta)
#             week_date = current_date - relativedelta(weeks=delta)
#             month_date = current_date - relativedelta(months=delta)
#             year_date = current_date - relativedelta(years=delta)
#             day_start, day_end = get_day_start_and_end_datetime(yesterday_date)
#             week_start, week_end = get_week_start_and_end_datetime(week_date)
#             month_start, month_end = get_month_start_and_end_datetime(month_date)
#             year_start, year_end = get_year_start_and_end_datetime(year_date)

#             query = Q()

#             if inst:
#                 query &= Q(issuer_institution_name=inst.bespokeCode) | Q(
#                     issuer_institution_name=inst.tlaCode
#                 )

#             queryset = Transactions.objects.using("etl_db").filter(
#                 query, transaction_time__range=[day_start, day_end]
#             )
#             bespoke_success_amount = (
#                 queryset.filter(status_code="00").aggregate(Sum("total_amount"))[
#                     "total_amount__sum"
#                 ]
#                 or 0
#             )
#             bespoke_failed_amount = (
#                 queryset.filter(status_code="03").aggregate(Sum("total_amount"))[
#                     "total_amount__sum"
#                 ]
#                 or 0
#             )
#             tla_suc_amt = (
#                 queryset.filter(
#                     transaction_status="1", department="processing"
#                 ).aggregate(Sum("amount"))["amount__sum"]
#                 or 0
#             )
#             tla_fail_amt = (
#                 queryset.filter(department="processing")
#                 .exclude(transaction_status="1")
#                 .aggregate(Sum("amount"))["amount__sum"]
#                 or 0
#             )

#             yesterday_total_data = dict()
#             yesterday_total_data["day"] = day_start.strftime("%d %b")
#             yesterday_total_data["successAmount"] = bespoke_success_amount + tla_suc_amt
#             yesterday_total_data["failedAmount"] = bespoke_failed_amount + tla_fail_amt

#             yesterday.append(yesterday_total_data)

#             queryset = Transactions.objects.using("etl_db").filter(
#                 query, transaction_time__range=[week_start, week_end]
#             )
#             bespoke_success_amount = (
#                 queryset.filter(status_code="00").aggregate(Sum("total_amount"))[
#                     "total_amount__sum"
#                 ]
#                 or 0
#             )
#             bespoke_failed_amount = (
#                 queryset.filter(status_code="03").aggregate(Sum("total_amount"))[
#                     "total_amount__sum"
#                 ]
#                 or 0
#             )
#             tla_suc_amt = (
#                 queryset.filter(
#                     transaction_status="1", department="processing"
#                 ).aggregate(Sum("amount"))["amount__sum"]
#                 or 0
#             )
#             tla_fail_amt = (
#                 queryset.filter(department="processing")
#                 .exclude(transaction_status="1")
#                 .aggregate(Sum("amount"))["amount__sum"]
#                 or 0
#             )
#             week = week + 1
#             week_total_data = dict()
#             week_total_data["week"] = (
#                 f"week {week}"  # f'{week_start.strftime("%d %b")} - {week_end.strftime("%d %b")}'
#             )
#             week_total_data["successAmount"] = bespoke_success_amount + tla_suc_amt
#             week_total_data["failedAmount"] = bespoke_failed_amount + tla_fail_amt

#             weekly.append(week_total_data)

#             queryset = Transactions.objects.using("etl_db").filter(
#                 query, transaction_time__range=[month_start, month_end]
#             )
#             bespoke_success_amount = (
#                 queryset.filter(status_code="00").aggregate(Sum("total_amount"))[
#                     "total_amount__sum"
#                 ]
#                 or 0
#             )
#             bespoke_failed_amount = (
#                 queryset.filter(status_code="03").aggregate(Sum("total_amount"))[
#                     "total_amount__sum"
#                 ]
#                 or 0
#             )
#             tla_suc_amt = (
#                 queryset.filter(
#                     transaction_status="1", department="processing"
#                 ).aggregate(Sum("amount"))["amount__sum"]
#                 or 0
#             )
#             tla_fail_amt = (
#                 queryset.filter(department="processing")
#                 .exclude(transaction_status="1")
#                 .aggregate(Sum("amount"))["amount__sum"]
#                 or 0
#             )

#             month_total_data = dict()
#             month_total_data["month"] = month_start.strftime("%b")
#             month_total_data["successAmount"] = bespoke_success_amount + tla_suc_amt
#             month_total_data["failedAmount"] = bespoke_failed_amount + tla_fail_amt

#             monthly.append(month_total_data)

#             queryset = Transactions.objects.using("etl_db").filter(
#                 query, transaction_time__range=[year_start, year_end]
#             )
#             bespoke_success_amount = (
#                 queryset.filter(status_code="00").aggregate(Sum("total_amount"))[
#                     "total_amount__sum"
#                 ]
#                 or 0
#             )
#             bespoke_failed_amount = (
#                 queryset.filter(status_code="03").aggregate(Sum("total_amount"))[
#                     "total_amount__sum"
#                 ]
#                 or 0
#             )
#             tla_suc_amt = (
#                 queryset.filter(
#                     transaction_status="1", department="processing"
#                 ).aggregate(Sum("amount"))["amount__sum"]
#                 or 0
#             )
#             tla_fail_amt = (
#                 queryset.filter(department="processing")
#                 .exclude(transaction_status="1")
#                 .aggregate(Sum("amount"))["amount__sum"]
#                 or 0
#             )

#             year_total_data = dict()
#             year_total_data["year"] = year_start.strftime("%Y")
#             year_total_data["successAmount"] = bespoke_success_amount + tla_suc_amt
#             year_total_data["failedAmount"] = bespoke_failed_amount + tla_fail_amt

#             yearly.append(year_total_data)

#         result["yesterday"] = yesterday
#         result["weekly"] = weekly
#         result["monthly"] = monthly
#         result["yearly"] = yearly
#         cache.set(key=cache_key, value=result, timeout=cache_timeout)
#         connections["etl_db"].close()

#     return result


# def local_to_foreign_transaction_report(user, inst_id, duration):
#     user_profile = UserDetail.objects.get(user=user)
#     today = timezone.now()

#     if duration == "" or None:
#         duration = "yesterday"

#     if duration == "thisMonth":
#         start_date, end_date = get_month_start_and_end_datetime(today)
#     elif duration == "thisWeek":
#         start_date, end_date = get_week_start_and_end_datetime(today)
#     elif duration == "thisYear":
#         start_date, end_date = get_year_start_and_end_datetime(today)
#     else:
#         yesterday = get_previous_date(today, 1)
#         start_date, end_date = get_day_start_and_end_datetime(yesterday)

#     if user_profile.institution:
#         inst = user_profile.institution
#     elif inst_id:
#         inst = Institution.objects.get(id=inst_id)
#     else:
#         inst = None

#     if inst is not None:
#         cache_key = f"{str(inst.name).replace(' ', '')}_local_to_foreign_transaction_report_{duration}"
#     else:
#         cache_key = f"all_local_to_foreign_transaction_report_{duration}"

#     result = cache.get(cache_key)

#     if result is None:
#         if inst is not None:
#             bespoke_isnt = Q(
#                 issuer_institution_name=inst.bespokeCode,
#                 transaction_time__range=[start_date, end_date],
#             )
#             tla_isnt = Q(
#                 issuer_institution_name=inst.tlaCode,
#                 transaction_time__range=[start_date, end_date],
#             )
#         else:
#             bespoke_isnt = Q(
#                 department="bespoke", transaction_time__range=[start_date, end_date]
#             )
#             tla_isnt = Q(
#                 department="processing", transaction_time__range=[start_date, end_date]
#             )

#         bespoke_local_issuer_trans = (
#             Transactions.objects.using("etl_db")
#             .filter(bespoke_isnt, issuer_country="566")
#             .aggregate(Sum("total_amount"))["total_amount__sum"]
#             or 0
#         )
#         tla_local_issuer_trans = (
#             Transactions.objects.using("etl_db")
#             .filter(tla_isnt, issuer_country="566")
#             .aggregate(Sum("amount"))["amount__sum"]
#             or 0
#         )
#         bespoke_foreign_issuer_trans = (
#             Transactions.objects.using("etl_db")
#             .filter(bespoke_isnt, issuer_country__isnull=False)
#             .exclude(issuer_country="566")
#             .aggregate(Sum("total_amount"))["total_amount__sum"]
#             or 0
#         )
#         tla_foreign_issuer_trans = (
#             Transactions.objects.using("etl_db")
#             .filter(tla_isnt, issuer_country__isnull=False)
#             .exclude(issuer_country="566")
#             .aggregate(Sum("amount"))["amount__sum"]
#             or 0
#         )
#         bespoke_local_aquirer_trans = (
#             Transactions.objects.using("etl_db")
#             .filter(bespoke_isnt, acquirer_country="566")
#             .aggregate(Sum("total_amount"))["total_amount__sum"]
#             or 0
#         )
#         tla_local_aquirer_trans = (
#             Transactions.objects.using("etl_db")
#             .filter(tla_isnt, acquirer_country="566")
#             .aggregate(Sum("amount"))["amount__sum"]
#             or 0
#         )
#         bespoke_foreign_aquirer_trans = (
#             Transactions.objects.using("etl_db")
#             .filter(bespoke_isnt, acquirer_country__isnull=False)
#             .exclude(acquirer_country="566")
#             .aggregate(Sum("total_amount"))["total_amount__sum"]
#             or 0
#         )
#         tla_foreign_aquirer_trans = (
#             Transactions.objects.using("etl_db")
#             .filter(tla_isnt, acquirer_country__isnull=False)
#             .exclude(acquirer_country="566")
#             .aggregate(Sum("amount"))["amount__sum"]
#             or 0
#         )
#         # print("ppppppppp",tla_foreign_aquirer_trans,bespoke_foreign_issuer_trans)
#         result = {
#             "issuerTransaction": {
#                 "local": bespoke_local_issuer_trans + tla_local_issuer_trans,
#                 "foreign": bespoke_foreign_issuer_trans + tla_foreign_issuer_trans,
#             },
#             "aquirerTransaction": {
#                 "local": bespoke_local_aquirer_trans + tla_local_aquirer_trans,
#                 "foreign": bespoke_foreign_aquirer_trans + tla_foreign_aquirer_trans,
#             },
#         }
#         cache.set(key=cache_key, value=result, timeout=cache_timeout)
#         connections["etl_db"].close()

#     return result


def transaction_by_channel_report(user, inst_id, duration):
    user_profile = UserDetail.objects.get(user=user)
    today = timezone.now()

    if duration == "" or duration is None:
        duration = "yesterday"

    if duration == "thisMonth":
        start_date, end_date = get_month_start_and_end_datetime(today)
    elif duration == "thisWeek":
        start_date, end_date = get_week_start_and_end_datetime(today)
    elif duration == "thisYear":
        start_date, end_date = get_year_start_and_end_datetime(today)
    else:
        yesterday = get_previous_date(today, 1)
        start_date, end_date = get_day_start_and_end_datetime(yesterday)

    if user_profile.institution:
        inst = user_profile.institution
    elif inst_id:
        inst = Institution.objects.get(id=inst_id)
    else:
        inst = None

    if inst is not None:
        cache_key = (
            f"{str(inst.name).replace(' ', '')}_transaction_by_channel_{duration}"
        )
    else:
        cache_key = f"all_transaction_by_channel_{duration}"

    result = cache.get(cache_key)
    if result is None:
        with connections["etl_db"].cursor() as cursor:
            if inst is None:
                cursor.execute(
                    """
                    SELECT
                        SUM(CASE WHEN channel = 'ussd' THEN 1 ELSE 0 END) AS bespoke_ussd,
                        SUM(CASE WHEN channel = 'bankussd' THEN 1 ELSE 0 END) AS bespoke_ussd_bank,
                        SUM(CASE WHEN channel = 'pos' THEN 1 ELSE 0 END) AS bespoke_pos,
                        SUM(CASE WHEN channel = 'web' THEN 1 ELSE 0 END) AS bespoke_web,
                        SUM(CASE WHEN channel = 'atm' THEN 1 ELSE 0 END) AS bespoke_atm,
                        SUM(CASE WHEN channel = 'agency' THEN 1 ELSE 0 END) AS bespoke_agency,
                        SUM(CASE WHEN channel = 'mobile' THEN 1 ELSE 0 END) AS bespoke_mobile,
                        SUM(CASE WHEN channel = '2' AND department = 'processing' THEN 1 ELSE 0 END) AS tla_pos,
                        SUM(CASE WHEN channel = '2' AND department = 'processing' THEN 1 ELSE 0 END) AS tla_web,
                        SUM(CASE WHEN channel = '1' AND department = 'processing' THEN 1 ELSE 0 END) AS tla_atm
                    FROM transactions
                    WHERE transaction_time BETWEEN %s AND %s
                """,
                    [start_date, end_date],
                )
                (
                    bespoke_ussd,
                    bespoke_ussd_bank,
                    bespoke_pos,
                    bespoke_web,
                    bespoke_atm,
                    bespoke_agency,
                    bespoke_mobile,
                    tla_pos,
                    tla_web,
                    tla_atm,
                ) = cursor.fetchone()
            else:
                cursor.execute(
                    """
                    SELECT
                        SUM(CASE WHEN channel = 'ussd' AND issuer_institution_name = %s THEN 1 ELSE 0 END) AS bespoke_ussd,
                        SUM(CASE WHEN channel = 'bankussd' AND issuer_institution_name = %s THEN 1 ELSE 0 END) AS bespoke_ussd_bank,
                        SUM(CASE WHEN channel = 'pos' AND issuer_institution_name = %s THEN 1 ELSE 0 END) AS bespoke_pos,
                        SUM(CASE WHEN channel = 'web' AND issuer_institution_name = %s THEN 1 ELSE 0 END) AS bespoke_web,
                        SUM(CASE WHEN channel = 'atm' AND issuer_institution_name = %s THEN 1 ELSE 0 END) AS bespoke_atm,
                        SUM(CASE WHEN channel = 'agency' AND issuer_institution_name = %s THEN 1 ELSE 0 END) AS bespoke_agency,
                        SUM(CASE WHEN channel = 'mobile' AND issuer_institution_name = %s THEN 1 ELSE 0 END) AS bespoke_mobile,
                        SUM(CASE WHEN channel = '2' AND department = 'processing' AND issuer_institution_name = %s THEN 1 ELSE 0 END) AS tla_pos,
                        SUM(CASE WHEN channel = '2' AND department = 'processing' AND issuer_institution_name = %s THEN 1 ELSE 0 END) AS tla_web,
                        SUM(CASE WHEN channel = '1' AND department = 'processing' AND issuer_institution_name = %s THEN 1 ELSE 0 END) AS tla_atm
                    FROM transactions
                    WHERE transaction_time BETWEEN %s AND %s
                """,
                    [
                        inst.bespokeCode,
                        inst.bespokeCode,
                        inst.bespokeCode,
                        inst.bespokeCode,
                        inst.bespokeCode,
                        inst.bespokeCode,
                        inst.bespokeCode,
                        inst.tlaCode,
                        inst.tlaCode,
                        inst.tlaCode,
                        start_date,
                        end_date,
                    ],
                )
                (
                    bespoke_ussd,
                    bespoke_ussd_bank,
                    bespoke_pos,
                    bespoke_web,
                    bespoke_atm,
                    bespoke_agency,
                    bespoke_mobile,
                    tla_pos,
                    tla_web,
                    tla_atm,
                ) = cursor.fetchone()

        pos_transactions = bespoke_pos + tla_pos
        web_transactions = bespoke_web + tla_web
        atm_transactions = bespoke_atm + tla_atm

        result = {
            "posTransactionCount": pos_transactions,
            "webTransactionCount": web_transactions,
            "atmTransactionCount": atm_transactions,
            "agencyTransactionCount": bespoke_agency,
            "mobileTransactionCount": bespoke_mobile,
            "ussdTransactionCount": bespoke_ussd,
            "bankUssdTransactionCount": bespoke_ussd_bank,
        }
        cache.set(key=cache_key, value=result, timeout=cache_timeout)
        connections["etl_db"].close()
    return result


# def transaction_trends_report(user, inst_id):
#     current_date = timezone.now()
#     profile = UserDetail.objects.get(user=user)
#     yesterday = []
#     monthly = []
#     yearly = []
#     weekly = []

#     if profile.institution:
#         inst = profile.institution
#     elif inst_id is not None:
#         inst = Institution.objects.get(id=inst_id)
#     else:
#         inst = None

#     if inst is not None:
#         cache_key = f"{str(inst.name).replace(' ', '')}_transaction_trends_report"
#     else:
#         cache_key = "all_transaction_trends_report"

#     if cache.get(cache_key):
#         result = cache.get(cache_key)
#     else:
#         result = dict()
#         week = 0
#         for delta in range(6, -1, -1):
#             yesterday_date = current_date - relativedelta(days=delta)
#             week_date = current_date - relativedelta(weeks=delta)
#             month_date = current_date - relativedelta(months=delta)
#             year_date = current_date - relativedelta(years=delta)
#             day_start, day_end = get_day_start_and_end_datetime(yesterday_date)
#             week_start, week_end = get_week_start_and_end_datetime(week_date)
#             month_start, month_end = get_month_start_and_end_datetime(month_date)
#             year_start, year_end = get_year_start_and_end_datetime(year_date)

#             query = Q()

#             if inst:
#                 query &= Q(issuer_institution_name=inst.bespokeCode) | Q(
#                     issuer_institution_name=inst.tlaCode
#                 )

#             queryset = Transactions.objects.using("etl_db").filter(
#                 query, transaction_time__range=[day_start, day_end]
#             )
#             total_count = queryset.count()

#             yesterday_total_data = dict()
#             yesterday_total_data["day"] = day_start.strftime("%d %b")
#             yesterday_total_data["totalTrendCount"] = total_count

#             yesterday.append(yesterday_total_data)

#             queryset = Transactions.objects.using("etl_db").filter(
#                 query, transaction_time__range=[week_start, week_end]
#             )
#             total_count = queryset.count()
#             week = week + 1
#             week_total_data = dict()
#             week_total_data["week"] = (
#                 f"week {week}"  # f"{week_start.strftime('%d %b')} - {week_end.strftime('%d %b')}"
#             )
#             week_total_data["totalTrendCount"] = total_count

#             weekly.append(week_total_data)

#             queryset = Transactions.objects.using("etl_db").filter(
#                 query, transaction_time__range=[month_start, month_end]
#             )
#             total_count = queryset.count()

#             month_total_data = dict()
#             month_total_data["month"] = month_start.strftime("%b")
#             month_total_data["totalTrendCount"] = total_count

#             monthly.append(month_total_data)

#             queryset = Transactions.objects.using("etl_db").filter(
#                 query, transaction_time__range=[year_start, year_end]
#             )
#             total_count = queryset.count()

#             year_total_data = dict()
#             year_total_data["year"] = year_start.strftime("%Y")
#             year_total_data["totalTrendCount"] = total_count

#             yearly.append(year_total_data)

#         result["daily"] = yesterday
#         result["weekly"] = weekly
#         result["monthly"] = monthly
#         result["yearly"] = yearly
#         cache.set(key=cache_key, value=result, timeout=cache_timeout)
#         connections["etl_db"].close()

#     return result


# def card_processed_count(user, inst_id, duration):
#     today = timezone.now()
#     user_profile = UserDetail.objects.get(user=user)

#     if duration == "" or None:
#         duration = "yesterday"

#     if duration == "thisMonth":
#         start_date, end_date = get_month_start_and_end_datetime(today)
#         prev_start_date, prev_end_date = get_month_start_and_end_datetime(start_date)
#     elif duration == "thisYear":
#         start_date, end_date = get_year_start_and_end_datetime(today)
#         prev_start_date, prev_end_date = get_year_start_and_end_datetime(start_date)
#     else:
#         start_date, end_date = get_week_start_and_end_datetime(today)
#         prev_start_date, prev_end_date = get_week_start_and_end_datetime(start_date)

#     if inst_id:
#         inst = Institution.objects.get(id=inst_id)
#     elif user_profile.institution:
#         inst = user_profile.institution
#     else:
#         inst = None

#     if inst is None:
#         cache_key = f"card_processed_count_{duration}"
#     else:
#         cache_key = f"{str(inst.name).replace(' ', '')}_card_processed_count_{duration}"

#     result = cache.get(cache_key)
#     if result is None:
#         query = Q(tcard_createdate__range=[start_date, end_date], pan__isnull=False)
#         prev_query = Q(
#             tcard_createdate__range=[prev_start_date, prev_end_date], pan__isnull=False
#         )
#         if inst:
#             query &= Q(branch__iexact=inst.issuingCode)
#             prev_query &= Q(branch__iexact=inst.issuingCode)

#         total_card = CardAccountDetailsIssuing.objects.using("etl_db").filter(query)
#         prev_total_card = CardAccountDetailsIssuing.objects.using("etl_db").filter(
#             prev_query
#         )
#         inactive_list = [5, 6, 7, 8]
#         total_count = total_card.count()
#         total_valid = total_card.filter(tcard_sign_stat=4).count()
#         prev_total_valid = prev_total_card.filter(tcard_sign_stat=4).count()
#         total_expired = total_card.filter(tcard_sign_stat=10).count()
#         prev_total_expired = prev_total_card.filter(tcard_sign_stat=10).count()
#         total_invalid = total_card.filter(tcard_sign_stat__in=inactive_list).count()
#         prev_total_invalid = prev_total_card.filter(
#             tcard_sign_stat__in=inactive_list
#         ).count()
#         total_blocked = total_card.filter(tcard_sign_stat=18).count()
#         prev_total_blocked = prev_total_card.filter(tcard_sign_stat=18).count()

#         valid = expired = invalid = blocked = "-"
#         if total_valid > prev_total_valid:
#             valid = "+"
#         if total_expired > prev_total_expired:
#             expired = "+"
#         if total_invalid > prev_total_invalid:
#             invalid = "+"
#         if total_blocked > prev_total_blocked:
#             blocked = "+"

#         result = {
#             "totalCardProcessed": total_count,
#             "totalValidCards": {
#                 "count": total_valid,
#                 "percentage": calculate_percentage_change(
#                     prev_total_valid, total_valid
#                 ),
#                 "direction": valid,
#             },
#             "totalExpiredCards": {
#                 "count": total_expired,
#                 "percentage": calculate_percentage_change(
#                     prev_total_expired, total_expired
#                 ),
#                 "direction": expired,
#             },
#             "totalInvalidCards": {
#                 "count": total_invalid,
#                 "percentage": calculate_percentage_change(
#                     prev_total_invalid, total_invalid
#                 ),
#                 "direction": invalid,
#             },
#             "totalBlockedCards": {
#                 "count": total_blocked,
#                 "percentage": calculate_percentage_change(
#                     prev_total_blocked, total_blocked
#                 ),
#                 "direction": blocked,
#             },
#         }

#         cache.set(key=cache_key, value=result, timeout=cache_timeout)
#         connections["etl_db"].close()

#     return result


def get_card_counts(inst_id):
    query = """
           SELECT
            COUNT(*) FILTER (WHERE tcard_createdate = current_date) AS totalCardsToday,
            SUM(CASE 
                    WHEN tcard_sign_stat = 10 
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 1
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 > 99
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 107
                    THEN 1 
                    ELSE 0 
                END) AS TodayExpired,
            SUM(CASE 
                    WHEN tcard_sign_stat = 4 
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 1
                    THEN 1 
                    ELSE 0 
                END) AS TodayValidCard,
            SUM(CASE 
                    WHEN tcard_sign_stat IN (5, 6, 7, 8) 
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 1
                    THEN 1 
                    ELSE 0 
                END) AS TodayInvalidCard,
            SUM(CASE 
                    WHEN tcard_sign_stat = 18
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 1
                    THEN 1 
                    ELSE 0 
                END) AS TodayBlockCard,
            COUNT(*) FILTER (WHERE tcard_createdate >= (current_date - interval '7 days') AND tcard_createdate <= current_date) AS totalCardsThisWeek,
            SUM(CASE 
                    WHEN tcard_sign_stat = 10 
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 7
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 > 99
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 107
                    THEN 1 
                    ELSE 0 
                END) AS ThisWeekExpired,
            SUM(CASE 
                    WHEN tcard_sign_stat = 4 
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 7
                    THEN 1 
                    ELSE 0 
                END) AS ThisWeekValidCard,
            SUM(CASE 
                    WHEN tcard_sign_stat IN (5, 6, 7, 8) 
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 7
                    THEN 1 
                    ELSE 0 
                END) AS ThisWeekInvalidCard,
            SUM(CASE 
                    WHEN tcard_sign_stat = 18
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 7
                    THEN 1 
                    ELSE 0 
                END) AS ThisWeekBlockCard,
            COUNT(*) FILTER (WHERE tcard_createdate >= date_trunc('month', current_date) AND tcard_createdate <= current_date) AS totalCardsThisMonth,
            SUM(CASE 
                    WHEN tcard_sign_stat = 10 
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 30
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 > 99
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 107
                    THEN 1 
                    ELSE 0 
                END) AS ThisMonthExpired,
            SUM(CASE 
                    WHEN tcard_sign_stat = 4 
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 30
                    THEN 1 
                    ELSE 0 
                END) AS ThisMonthValidCard,
            SUM(CASE 
                    WHEN tcard_sign_stat IN (5, 6, 7, 8) 
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 30
                    THEN 1 
                    ELSE 0 
                END) AS ThisMonthInvalidCard,
            SUM(CASE 
                    WHEN tcard_sign_stat = 18
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 30
                    THEN 1 
                    ELSE 0 
                END) AS ThisMonthBlockCard,
            SUM(CASE 
                    WHEN tcard_sign_stat = 10 
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 >= 1
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 2
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 > 99
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 107
                    THEN 1 
                    ELSE 0 
                END) AS prevDayExpired,
		     SUM(CASE 
                    WHEN tcard_sign_stat = 10 
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 >= 7
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 14
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 > 99
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 107
                    THEN 1 
                    ELSE 0 
                END) AS prevWeekExpired,
     SUM(CASE 
                    WHEN tcard_sign_stat = 10 
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 >= 30
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 60
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 > 99
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 107
                    THEN 1 
                    ELSE 0 
                END) AS prevMonthExpired,
            SUM(CASE 
                    WHEN tcard_sign_stat = 4 
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 >= 7
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 14
                    THEN 1 
                    ELSE 0 
                END) AS prevWeekValid,
            SUM(CASE 
                    WHEN tcard_sign_stat = 4 
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 >= 30
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 60
                    THEN 1 
                    ELSE 0 
                END) AS prevMonthValid,
            SUM(CASE 
                    WHEN tcard_sign_stat = 4 
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 >= 1
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 2
                    THEN 1 
                    ELSE 0 
                END) AS prevDayValid,
            SUM(CASE 
                    WHEN tcard_sign_stat IN (5, 6, 7, 8) 
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 >= 1
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 2
                    THEN 1 
                    ELSE 0 
                END) AS prevDayInvalid,
            SUM(CASE 
                    WHEN tcard_sign_stat IN (5, 6, 7, 8) 
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 >= 7
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 14
                    THEN 1 
                    ELSE 0 
                END) AS prevWeekInvalid,
            SUM(CASE 
                    WHEN tcard_sign_stat IN (5, 6, 7, 8) 
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 >= 30
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 60
                    THEN 1 
                    ELSE 0 
                END) AS prevMonthInvalid,
            SUM(CASE 
                    WHEN tcard_sign_stat = 18
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 >= 7
                         AND EXTRACT(EPOCH FROM (current_date - tcard_createdate)) / 86400 <= 14
                    THEN 1 
                    ELSE 0 
                END) AS prevTotalBlocked
        FROM card_account_details_issuing
        WHERE pan IS NOT null;
    """
    if inst_id:
        query += " AND branch = %s"
    else:
        query += ";"
    with connections["etl_db"].cursor() as cursor:
        cursor.execute(query, [inst_id])
        row = cursor.fetchone()

        (
            total_cards_today,
            today_expired,
            today_valid_card,
            today_invalid_card,
            today_block_card,
            total_cards_this_week,
            this_week_expired,
            this_week_valid_card,
            this_week_invalid_card,
            this_week_block_card,
            total_cards_this_month,
            this_month_expired,
            this_month_valid_card,
            this_month_invalid_card,
            this_month_block_card,
            prev_day_expired,
            prev_week_expired,
            prev_month_expired,
            prev_week_valid,
            prev_month_valid,
            prev_day_valid,
            prev_day_invalid,
            prev_week_invalid,
            prev_month_invalid,
            prev_total_blocked,
        ) = row

        # Calculate percentage changes
        percentage_change_valid_week = calculate_percentage_change(
            prev_week_valid, today_valid_card
        )
        percentage_change_invalid_week = calculate_percentage_change(
            prev_week_invalid, today_invalid_card
        )
        percentage_change_valid_month = calculate_percentage_change(
            prev_month_valid, today_valid_card
        )
        percentage_change_invalid_month = calculate_percentage_change(
            prev_month_invalid, today_invalid_card
        )

        valid = expired = invalid = blocked = "-"
        if today_valid_card > prev_week_valid:
            valid = "+"
        if today_expired > prev_day_expired:
            expired = "+"
        if today_invalid_card > prev_day_invalid:
            invalid = "+"
        if today_block_card > prev_total_blocked:
            blocked = "+"

        result = {
            "totalCardtoday": total_cards_today,
            "totalCardWeek": total_cards_this_week,
            "totalCardMonth": total_cards_this_month,
            "todayValidCards": {
                "count": today_valid_card,
                "percentage": percentage_change_valid_week,
                "direction": valid,
            },
            "todayExpiredCards": {
                "count": today_expired,
                "percentage": calculate_percentage_change(
                    prev_day_expired, today_expired
                ),
                "direction": expired,
            },
            "todayInvalidCards": {
                "count": today_invalid_card,
                "percentage": percentage_change_invalid_week,
                "direction": invalid,
            },
            "todayBlockedCards": {
                "count": today_block_card,
                "percentage": calculate_percentage_change(
                    prev_total_blocked, today_block_card
                ),
                "direction": blocked,
            },
            "weeklyValidCards": {
                "count": this_week_valid_card,
                "percentage": percentage_change_valid_week,
                "direction": valid,
            },
            "weeklyExpiredCards": {
                "count": this_week_expired,
                "percentage": calculate_percentage_change(
                    prev_week_expired, this_week_expired
                ),
                "direction": expired,
            },
            "weeklyInvalidCards": {
                "count": this_week_invalid_card,
                "percentage": percentage_change_invalid_week,
                "direction": invalid,
            },
            "weeklyBlockedCards": {
                "count": this_week_block_card,
                "percentage": calculate_percentage_change(
                    prev_total_blocked, this_week_block_card
                ),
                "direction": blocked,
            },
            "monthlyValidCards": {
                "count": this_month_valid_card,
                "percentage": percentage_change_valid_month,
                "direction": valid,
            },
            "monthlyExpiredCards": {
                "count": this_month_expired,
                "percentage": calculate_percentage_change(
                    prev_month_expired, this_month_expired
                ),
                "direction": expired,
            },
            "monthlyInvalidCards": {
                "count": this_month_invalid_card,
                "percentage": percentage_change_invalid_month,
                "direction": invalid,
            },
            "monthlyBlockedCards": {
                "count": this_month_block_card,
                "percentage": calculate_percentage_change(
                    prev_total_blocked, this_month_block_card
                ),
                "direction": blocked,
            },
            "prevDayExpiredCards": prev_day_expired,
            "prevWeekExpiredCards": prev_week_expired,
            "prevMonthExpiredCards": prev_month_expired,
            "prevDayValidCards": prev_day_valid,
            "prevDayInvalidCards": prev_day_invalid,
            "prevWeekInvalidCards": prev_week_invalid,
            "prevMonthInvalidCards": prev_month_invalid,
            "prevTotalBlockedCards": prev_total_blocked,
        }

    return result


def card_processed_count(user, inst_id, duration):
    today = timezone.now()
    user_profile = UserDetail.objects.get(user=user)

    if inst_id:
        inst = Institution.objects.get(id=inst_id)
    elif user_profile.institution:
        inst = user_profile.institution
    else:
        inst = None

    if inst is None:
        cache_key = f"card_processed_count_{duration}"
    else:
        cache_key = f"{str(inst.name).replace(' ', '')}_card_processed_count_{duration}"

    result = cache.get(cache_key)
    if result is None:

        result = get_card_counts(inst.issuingCode if inst else None)

        cache.set(key=cache_key, value=result, timeout=cache_timeout)
        connections["etl_db"].close()

    return result


# def monthly_transaction_report(user, inst_id):
#     current_date = timezone.now()
#     user_profile = UserDetail.objects.get(user=user)
#     monthly = []

#     if inst_id:
#         inst = Institution.objects.get(id=inst_id)
#     elif user_profile.institution:
#         inst = user_profile.institution
#     else:
#         inst = None

#     if inst is None:
#         cache_key = f"all_monthly_transaction_report"
#     else:
#         cache_key = f"{str(inst.name).replace(' ', '')}_monthly_transaction_report"

#     result = cache.get(cache_key)

#     if result is None:
#         result = dict()
#         for delta in range(11, -1, -1):
#             month_date = current_date - relativedelta(months=delta)
#             month_start, month_end = get_month_start_and_end_datetime(month_date)

#             query = Q()

#             if inst:
#                 query &= Q(issuer_institution_name=inst.bespokeCode) | Q(
#                     issuer_institution_name=inst.tlaCode
#                 )

#             queryset = Transactions.objects.using("etl_db").filter(
#                 query, transaction_time__range=[month_start, month_end]
#             )
#             bespoke_total = queryset.aggregate(Sum("amount"))["amount__sum"] or 0
#             tla_total = (
#                 queryset.aggregate(Sum("total_amount"))["total_amount__sum"] or 0
#             )

#             month_total_data = dict()
#             month_total_data["month"] = (
#                 f"{month_start.strftime('%b')}-{str(month_start.year)[2:]}"
#             )
#             month_total_data["totalAmount"] = bespoke_total + tla_total

#             monthly.append(month_total_data)

#         result["monthly"] = monthly
#         cache.set(key=cache_key, value=result, timeout=cache_timeout)
#         connections["etl_db"].close()

#     return result


# def settlement_report():
#     today = timezone.now()
#     current_date = timezone.now()
#     cache_key = f"all_settlement_record_report"

#     if (result := cache.get(cache_key)) is not None:
#         return result

#     result = {"daily": [], "weekly": [], "monthly": [], "yearly": []}
#     # settlement_details = SettlementDetail.objects.using("etl_db").all()
#     # settlement_meb = SettlementMeb.objects.using("etl_db").all()
#     all_doc_no = SettlementDetail.objects.using("etl_db").values("docno")
#     total_settle_count_yearly = 0  # Initialize total settle count for years
#     total_un_settle_count_yearly = 0  # Initialize total un-settle count for years
#     total_settle_count_weekly = 0  # Initialize total settle count for weeks
#     total_un_settle_count_weekly = 0  # Initialize total un-settle count for weeks
#     total_settle_count_monthly = 0  # Initialize total settle count for months
#     total_un_settle_count_monthly = 0  # Initialize total un-settle count for months
#     total_settle_count_daily = 0  # Initialize total settle count for days
#     total_un_settle_count_daily = 0  # Initialize total un-settle count for days

#     for delta in range(6, -1, -1):
#         yesterday_date = current_date - relativedelta(days=delta)
#         week_date = current_date - relativedelta(weeks=delta)
#         month_date = current_date - relativedelta(months=delta)
#         year_date = current_date - relativedelta(years=delta)

#         day_start, day_end = get_day_start_and_end_datetime(yesterday_date)
#         week_start, week_end = get_week_start_and_end_datetime(week_date)
#         month_start, month_end = get_month_start_and_end_datetime(month_date)
#         year_start, year_end = get_year_start_and_end_datetime(year_date)

#         # daily = SettlementMeb.objects.using("etl_db").filter(docno__in=all_doc_no, createdate__range=[day_start, day_end])
#         # totalqueryset = SettlementMeb.objects.using("etl_db").filter(createdate__range=[day_start, day_end]).count()

#         for (
#             period,
#             start_date,
#             end_date,
#             total_settle_count_key,
#             total_un_settle_count_key,
#         ) in [
#             (
#                 "weekly",
#                 week_start,
#                 week_end,
#                 "total_settle_count_weekly",
#                 "total_un_settle_count_weekly",
#             ),
#             (
#                 "monthly",
#                 month_start,
#                 month_end,
#                 "total_settle_count_monthly",
#                 "total_un_settle_count_monthly",
#             ),
#             (
#                 "yearly",
#                 year_start,
#                 year_end,
#                 "total_settle_count_yearly",
#                 "total_un_settle_count_yearly",
#             ),
#             (
#                 "daily",
#                 day_start,
#                 day_end,
#                 "total_settle_count_daily",
#                 "total_un_settle_count_daily",
#             ),
#         ]:
#             total_count = (
#                 SettlementMeb.objects.using("etl_db")
#                 .filter(docno__in=all_doc_no, createdate__range=[start_date, end_date])
#                 .count()
#             )
#             totalqueryset = (
#                 SettlementMeb.objects.using("etl_db")
#                 .filter(createdate__range=[start_date, end_date])
#                 .count()
#             )
#             # total_count = queryset.count()

#             if period == "yearly":
#                 total_settle_count_yearly += total_count
#                 total_un_settle_count_yearly += totalqueryset - total_count
#             elif period == "weekly":
#                 total_settle_count_weekly += total_count
#                 total_un_settle_count_weekly += totalqueryset - total_count
#             elif period == "monthly":
#                 total_settle_count_monthly += total_count
#                 total_un_settle_count_monthly += totalqueryset - total_count
#             elif period == "daily":
#                 total_settle_count_daily += total_count
#                 total_un_settle_count_daily += totalqueryset - total_count

#     # Add total settle counts to the result
#     result["yearly"].append(
#         {
#             "year": "Total",
#             "totalSettleCount": total_settle_count_yearly,
#             "totalUnSettleCount": total_un_settle_count_yearly,  # Adjust this based on your specific requirements
#         }
#     )

#     result["weekly"].append(
#         {
#             "week": "Total",
#             "totalSettleCount": total_settle_count_weekly,
#             "totalUnSettleCount": total_un_settle_count_weekly,  # Adjust this based on your specific requirements
#         }
#     )

#     result["monthly"].append(
#         {
#             "month": "Total",
#             "totalSettleCount": total_settle_count_monthly,
#             "totalUnSettleCount": total_un_settle_count_monthly,  # Adjust this based on your specific requirements
#         }
#     )

#     result["daily"].append(
#         {
#             "day": "Total",
#             "totalSettleCount": total_settle_count_daily,
#             "totalUnSettleCount": total_un_settle_count_daily,  # Adjust this based on your specific requirements
#         }
#     )

#     cache.set(key=cache_key, value=result, timeout=cache_timeout)
#     connections["etl_db"].close()
#     return result


def terminalSourceAndDestination(value):
    cache_key = f"terminal_{value}_transaction"

    if cache.get(cache_key):
        result = cache.get(cache_key)
    else:
        if value == "source":
            result = list(
                UpConTerminalConfig.objects.using("etl_db")
                .filter(schema_or_table_name="terminal_message")
                .values("source")
                .distinct()
            )
        else:
            result = list(
                UpConTerminalConfig.objects.using("etl_db")
                .filter(schema_or_table_name="terminal_message")
                .values("destination")
                .distinct()
            )

        cache.set(key=cache_key, value=result, timeout=cache_timeout)

    return result


# def get_report_data(user, report_type, duration, inst_id, channel):
#     if report_type == "adminCount":
#         return total_admin_count_report(user, inst_id)
#     elif report_type == "transaction":
#         return total_transaction_count_and_value(user, inst_id, duration)
#     elif report_type == "channelCount":
#         return transaction_by_channel_report(user, inst_id, duration)
#     elif report_type == "channelAmount":
#         return transaction_status_per_channel(user, inst_id, channel)
#     elif report_type == "transactionTrend":
#         return transaction_trends_report(user, inst_id)
#     elif report_type == "transactionStatus":
#         return transaction_status(user, inst_id)
#     elif report_type == "localForeign":
#         return local_to_foreign_transaction_report(user, inst_id, duration)
#     elif report_type == "cardProcessing":
#         return card_processed_count(user, inst_id, duration)
#     elif report_type == "monthlyTransaction":
#         return monthly_transaction_report(user, inst_id)
#     elif report_type == "settlementTransaction":
#         return settlement_report()
#     else:
#         return None


# def get_card_processed_count_sql(duration):

#     query = f"""
#             WITH
#     current_dates AS (
#         SELECT
#             CASE
#                 WHEN :duration = 'thisWeek' THEN DATE_TRUNC('week', NOW())
#                 WHEN :duration = 'thisMonth' THEN DATE_TRUNC('month', NOW())
#                 WHEN :duration = 'thisYear' THEN DATE_TRUNC('year', NOW())
#                 ELSE DATE_TRUNC('week', NOW())
#             END AS start_date,
#             CASE
#                 WHEN :duration = 'thisWeek' THEN DATE_TRUNC('week', NOW()) + INTERVAL '6 days'
#                 WHEN :duration = 'thisMonth' THEN (DATE_TRUNC('month', NOW()) + INTERVAL '1 month - 1 day')::date
#                 WHEN :duration = 'thisYear' THEN (DATE_TRUNC('year', NOW()) + INTERVAL '1 year - 1 day')::date
#                 ELSE DATE_TRUNC('week', NOW()) + INTERVAL '6 days'
#             END AS end_date,
#             CASE
#                 WHEN :duration = 'thisWeek' THEN DATE_TRUNC('week', NOW()) - INTERVAL '1 week'
#                 WHEN :duration = 'thisMonth' THEN (DATE_TRUNC('month', NOW()) - INTERVAL '1 month')::date
#                 WHEN :duration = 'thisYear' THEN (DATE_TRUNC('year', NOW()) - INTERVAL '1 year')::date
#                 ELSE DATE_TRUNC('week', NOW()) - INTERVAL '1 week'
#             END AS prev_start_date,
#             CASE
#                 WHEN :duration = 'thisWeek' THEN DATE_TRUNC('week', NOW()) - INTERVAL '1 day'
#                 WHEN :duration = 'thisMonth' THEN (DATE_TRUNC('month', NOW()) - INTERVAL '1 day')::date
#                 WHEN :duration = 'thisYear' THEN (DATE_TRUNC('year', NOW()) - INTERVAL '1 day')::date
#                 ELSE DATE_TRUNC('week', NOW()) - INTERVAL '1 day'
#             END AS prev_end_date
#     ),
#     current_period_cards AS (
#         SELECT
#             tcard_sign_stat
#         FROM
#             card_account_details_issuing
#         WHERE
#             tcard_createdate BETWEEN (SELECT start_date FROM current_dates) AND (SELECT end_date FROM current_dates)
#             AND pan IS NOT NULL
#     ),
#     previous_period_cards AS (
#         SELECT
#             tcard_sign_stat
#         FROM
#             card_account_details_issuing
#         WHERE
#             tcard_createdate BETWEEN (SELECT prev_start_date FROM current_dates) AND (SELECT prev_end_date FROM current_dates)
#             AND pan IS NOT NULL

#     )
#     SELECT
#         (SELECT COUNT(*) FROM current_period_cards) AS totalCardProcessed,
#         (SELECT COUNT(*) FROM current_period_cards WHERE tcard_sign_stat = 4) AS totalValidCards_count,
#         (SELECT COALESCE(ROUND(
#             ((SELECT COUNT(*) FROM current_period_cards WHERE tcard_sign_stat = 4) -
#             (SELECT COUNT(*) FROM previous_period_cards WHERE tcard_sign_stat = 4)) /
#             NULLIF((SELECT COUNT(*) FROM previous_period_cards WHERE tcard_sign_stat = 4), 0) * 100, 2), 0)) AS totalValidCards_percentage,
#         (SELECT
#             CASE
#                 WHEN (SELECT COUNT(*) FROM current_period_cards WHERE tcard_sign_stat = 4) >
#                     (SELECT COUNT(*) FROM previous_period_cards WHERE tcard_sign_stat = 4) THEN '+'
#                 ELSE '-'
#             END
#         ) AS totalValidCards_direction,
#         (SELECT COUNT(*) FROM current_period_cards WHERE tcard_sign_stat = 10) AS totalExpiredCards_count,
#         (SELECT COALESCE(ROUND(
#             ((SELECT COUNT(*) FROM current_period_cards WHERE tcard_sign_stat = 10) -
#             (SELECT COUNT(*) FROM previous_period_cards WHERE tcard_sign_stat = 10)) /
#             NULLIF((SELECT COUNT(*) FROM previous_period_cards WHERE tcard_sign_stat = 10), 0) * 100, 2), 0)) AS totalExpiredCards_percentage,
#         (SELECT
#             CASE
#                 WHEN (SELECT COUNT(*) FROM current_period_cards WHERE tcard_sign_stat = 10) >
#                     (SELECT COUNT(*) FROM previous_period_cards WHERE tcard_sign_stat = 10) THEN '+'
#                 ELSE '-'
#             END
#         ) AS totalExpiredCards_direction,
#         (SELECT COUNT(*) FROM current_period_cards WHERE tcard_sign_stat IN (5, 6, 7, 8)) AS totalInvalidCards_count,
#         (SELECT COALESCE(ROUND(
#             ((SELECT COUNT(*) FROM current_period_cards WHERE tcard_sign_stat IN (5, 6, 7, 8)) -
#             (SELECT COUNT(*) FROM previous_period_cards WHERE tcard_sign_stat IN (5, 6, 7, 8))) /
#             NULLIF((SELECT COUNT(*) FROM previous_period_cards WHERE tcard_sign_stat IN (5, 6, 7, 8)), 0) * 100, 2), 0)) AS totalInvalidCards_percentage,
#         (SELECT
#             CASE
#                 WHEN (SELECT COUNT(*) FROM current_period_cards WHERE tcard_sign_stat IN (5, 6, 7, 8)) >
#                     (SELECT COUNT(*) FROM previous_period_cards WHERE tcard_sign_stat IN (5, 6, 7, 8)) THEN '+'
#                 ELSE '-'
#             END
#         ) AS totalInvalidCards_direction,
#         (SELECT COUNT(*) FROM current_period_cards WHERE tcard_sign_stat = 18) AS totalBlockedCards_count,
#         (SELECT COALESCE(ROUND(
#             ((SELECT COUNT(*) FROM current_period_cards WHERE tcard_sign_stat = 18) -
#             (SELECT COUNT(*) FROM previous_period_cards WHERE tcard_sign_stat = 18)) /
#             NULLIF((SELECT COUNT(*) FROM previous_period_cards WHERE tcard_sign_stat = 18), 0) * 100, 2), 0)) AS totalBlockedCards_percentage,
#         (SELECT
#             CASE
#                 WHEN (SELECT COUNT(*) FROM current_period_cards WHERE tcard_sign_stat = 18) >
#                     (SELECT COUNT(*) FROM previous_period_cards WHERE tcard_sign_stat = 18) THEN '+'
#                 ELSE '-'
#             END
#         ) AS totalBlockedCards_direction
#     LIMIT 1;

#     """


# def card_processed_count(user, inst_id, duration):
#     today = timezone.now()
#     user_profile = UserDetail.objects.get(user=user)

#     if duration == "" or duration is None:
#         duration = "yesterday"

#     if inst_id:
#         inst = Institution.objects.get(id=inst_id)
#     elif user_profile.institution:
#         inst = user_profile.institution
#     else:
#         inst = None

#     if inst is None:
#         cache_key = f"card_processed_count_{duration}"
#     else:
#         cache_key = f"{str(inst.name).replace(' ', '')}_card_processed_count_{duration}"

#     result = cache.get(cache_key)
#     if result is None:
#         inst_issuing_code = inst.issuingCode if inst else None
#         sql_query = get_card_processed_count_sql(inst_issuing_code, duration)

#         with connections["etl_db"].cursor() as cursor:
#             cursor.execute(sql_query)
#             row = cursor.fetchone()

#             result = {
#                 "totalCardProcessed": row[0],
#                 "totalValidCards": {
#                     "count": row[1],
#                     "percentage": row[2],
#                     "direction": "+" if row[3] > 0 else "-",
#                 },
#                 "totalExpiredCards": {
#                     "count": row[4],
#                     "percentage": row[5],
#                     "direction": "+" if row[6] > 0 else "-",
#                 },
#                 "totalInvalidCards": {
#                     "count": row[7],
#                     "percentage": row[8],
#                     "direction": "+" if row[9] > 0 else "-",
#                 },
#                 "totalBlockedCards": {
#                     "count": row[10],
#                     "percentage": row[11],
#                     "direction": "+" if row[12] > 0 else "-",
#                 },
#             }

#             cache.set(key=cache_key, value=result, timeout=cache_timeout)
#             connections["etl_db"].close()

#     return result


# current date to expired date from


"""
select count(*) as totalCards, 
                     sum(CASE WHEN (t_card_sign_stat = '1') && (EXTRACT(EPOCH FROM (current_date - createddate))/86400 > 99) && (EXTRACT(EPOCH FROM (current_date - createddate))/86400 > 107)) THEN 1 ELSE 0 END) AS ThisWeekExpired,
                    sum(CASE WHEN (t_card_sign_stat = '1') && (EXTRACT(EPOCH FROM (current_date - createddate))/86400 > 107) && (EXTRACT(EPOCH FROM (current_date - createddate))/86400 <= 114) THEN 1 ELSE 0 END) AS LastWeekExpired,
			         ((ThisWeekExpired/LastWeekExpired) * 100) as expiredPercentage
					 
                    sum(CASE WHEN (t_card_sign_stat = '1' || t_card_sign_stat  '2') && (EXTRACT(EPOCH FROM (current_date - createddate))/86400 > 99) && (EXTRACT(EPOCH FROM (current_date - createddate))/86400 > 107)) THEN 1 ELSE 0 END) AS ThisWeekExpired,
                    sum(CASE WHEN (t_card_sign_stat = '1'  || t_card_sign_stat  '2') && (EXTRACT(EPOCH FROM (current_date - createddate))/86400 > 107) && (EXTRACT(EPOCH FROM (current_date - createddate))/86400 <= 114) THEN 1 ELSE 0 END) AS LastWeekExpired,
			         ((ThisWeekExpired/LastWeekExpired) * 100) as expiredPercentage

              sum(CASE WHEN (t_card_sign_stat = '1') && (EXTRACT(EPOCH FROM (current_date - createddate))/86400 > 99) && (EXTRACT(EPOCH FROM (current_date - createddate))/86400 > 107)) THEN 1 ELSE 0 END) AS ThisWeekExpired,
                    sum(CASE WHEN (t_card_sign_stat = '1') && (EXTRACT(EPOCH FROM (current_date - createddate))/86400 > 107) && (EXTRACT(EPOCH FROM (current_date - createddate))/86400 <= 114) THEN 1 ELSE 0 END) AS LastWeekExpired,
			         ((ThisWeekExpired/LastWeekExpired) * 100) as expiredPercentage
					 
                    from table_name

"""


def transaction_status_per_channel_sql():
    """
    SELECT
        DATE(transaction_time) AS day,
        COUNT(*) AS total_count,
        SUM(CASE WHEN t.channel = 'POS' THEN t.total_amount ELSE 0 END) AS pos_amount,
        SUM(CASE WHEN t.channel = 'USSD' THEN t.total_amount ELSE 0 END) AS ussd_amount,
        SUM(CASE WHEN t.channel = 'atm' THEN t.total_amount ELSE 0 END) AS atm_amount,
        SUM(CASE WHEN t.channel = 'Web' THEN t.total_amount ELSE 0 END) AS web_amount,
        SUM(CASE WHEN t.channel = 'agency' THEN t.total_amount ELSE 0 END) AS agency_amount,
        SUM(CASE WHEN t.channel = 'BankUSSD' THEN t.total_amount ELSE 0 END) AS bankussd_amount,
        SUM(CASE WHEN t.channel = 'Mobile' THEN t.total_amount ELSE 0 END) AS mobileapp_amount,
        SUM(CASE WHEN t.transaction_status = '00' THEN t.total_amount ELSE 0 END) AS total_success_amount,
        SUM(CASE WHEN t.transaction_status = '03' THEN t.total_amount ELSE 0 END) AS total_failed_amount,
        SUM(CASE WHEN t.transaction_status = '05' THEN t.total_amount ELSE 0 END) AS total_declined_amount
    FROM
        Transactions t
    WHERE
        DATE(transaction_time) BETWEEN DATE(NOW() - INTERVAL 6 DAY) AND DATE(NOW())
    GROUP BY
        DATE(transaction_time)
    ORDER BY
        DATE(transaction_time);

    """
    """
    SELECT
    WEEK(transaction_time) AS week,
    COUNT(*) AS total_count,
    SUM(CASE WHEN t.channel = 'POS' THEN t.total_amount ELSE 0 END) AS pos_amount,
    SUM(CASE WHEN t.channel = 'USSD' THEN t.total_amount ELSE 0 END) AS ussd_amount,
    SUM(CASE WHEN t.channel = 'atm' THEN t.total_amount ELSE 0 END) AS atm_amount,
    SUM(CASE WHEN t.channel = 'Web' THEN t.total_amount ELSE 0 END) AS web_amount,
    SUM(CASE WHEN t.channel = 'agency' THEN t.total_amount ELSE 0 END) AS agency_amount,
    SUM(CASE WHEN t.channel = 'BankUSSD' THEN t.total_amount ELSE 0 END) AS bankussd_amount,
    SUM(CASE WHEN t.channel = 'Mobile' THEN t.total_amount ELSE 0 END) AS mobileapp_amount,
    SUM(CASE WHEN t.transaction_status = '00' THEN t.total_amount ELSE 0 END) AS total_success_amount,
    SUM(CASE WHEN t.transaction_status = '03' THEN t.total_amount ELSE 0 END) AS total_failed_amount,
    SUM(CASE WHEN t.transaction_status = '05' THEN t.total_amount ELSE 0 END) AS total_declined_amount
FROM
    Transactions t
WHERE
    WEEK(transaction_time) BETWEEN WEEK(NOW() - INTERVAL 6 WEEK) AND WEEK(NOW())
GROUP BY
    WEEK(transaction_time)
ORDER BY
    WEEK(transaction_time);

    """

    """
SELECT
    DATE(transaction_time) AS day,
    COUNT(*) AS total_count,
    SUM(CASE WHEN channel = 'POS' THEN total_amount ELSE 0 END) AS pos_amount,
    SUM(CASE WHEN channel = 'USSD' THEN total_amount ELSE 0 END) AS ussd_amount,
    SUM(CASE WHEN channel = 'atm' THEN total_amount ELSE 0 END) AS atm_amount,
    SUM(CASE WHEN channel = 'Web' THEN total_amount ELSE 0 END) AS web_amount,
    SUM(CASE WHEN channel = 'agency' THEN total_amount ELSE 0 END) AS agency_amount,
    SUM(CASE WHEN channel = 'BankUSSD' THEN total_amount ELSE 0 END) AS bankussd_amount,
    SUM(CASE WHEN channel = 'Mobile' THEN total_amount ELSE 0 END) AS mobileapp_amount,
    SUM(CASE WHEN transaction_status = '00' THEN total_amount ELSE 0 END) AS total_success_amount,
    SUM(CASE WHEN transaction_status = '03' THEN total_amount ELSE 0 END) AS total_failed_amount,
    SUM(CASE WHEN transaction_status = '05' THEN total_amount ELSE 0 END) AS total_declined_amount
FROM
    Transactions
WHERE
    DATE(transaction_time) >= DATE_TRUNC('month', CURRENT_DATE)
    AND DATE(transaction_time) < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'
GROUP BY
    DATE(transaction_time)
ORDER BY
    DATE(transaction_time);


    """


def monthly_transaction_report(user, inst_id):
    from django.db import connection

    current_date = timezone.now()
    user_profile = UserDetail.objects.get(user=user)
    monthly = []

    if inst_id:
        inst = Institution.objects.get(id=inst_id)
    elif user_profile.institution:
        inst = user_profile.institution
    else:
        inst = None

    if inst is None:
        cache_key = f"all_monthly_transaction_report"
    else:
        cache_key = f"{str(inst.name).replace(' ', '')}_monthly_transaction_report"

    result = cache.get(cache_key)

    if result is None:
        with connections["etl_db"].cursor() as cursor:
            if inst:
                query = """
                        WITH months AS (
                        SELECT TO_CHAR(date_trunc('month', CURRENT_DATE) - INTERVAL '1 month' * s.a, 'Mon-YY') AS month,
                        date_trunc('month', CURRENT_DATE) - INTERVAL '1 month' * s.a AS month_date
                        FROM generate_series(0, 5) AS s(a)
                    ),
                    transaction_totals AS (
                        SELECT TO_CHAR(transaction_time, 'Mon-YY') AS month, 
                        SUM(amount) + SUM(total_amount) AS total_amount
                        FROM transactions
                        WHERE transaction_time >= date_trunc('month', CURRENT_DATE) - INTERVAL '6 months'
                        AND (issuer_institution_name = %s OR issuer_institution_name = %s)
                        GROUP BY TO_CHAR(transaction_time, 'Mon-YY')
                    )
                    SELECT m.month,
                        COALESCE(t.total_amount, 0) AS total_amount
                    FROM months m
                    LEFT JOIN transaction_totals t ON m.month = t.month
                    ORDER BY m.month_date;
                """
                query = """
                SELECT 
                    TO_CHAR(transaction_time, 'Mon-YY') AS month, 
                    SUM(amount) + SUM(total_amount) AS total_amount
                FROM transactions
                WHERE (issuer_institution_name = %s OR issuer_institution_name = %s)
                GROUP BY TO_CHAR(transaction_time, 'Mon-YY')
                ORDER BY MIN(transaction_time);
                """
                params = [inst.bespokeCode, inst.tlaCode]
            else:
                query = """
                    WITH months AS (
                        SELECT TO_CHAR(date_trunc('month', CURRENT_DATE) - INTERVAL '1 month' * s.a, 'Mon-YY') AS month,
                            date_trunc('month', CURRENT_DATE) - INTERVAL '1 month' * s.a AS month_date
                        FROM generate_series(0, 5) AS s(a)
                    ),
                    transaction_totals AS (
                        SELECT TO_CHAR(transaction_time, 'Mon-YY') AS month, 
                            SUM(amount) + SUM(total_amount) AS total_amount
                        FROM transactions
                        WHERE transaction_time >= date_trunc('month', CURRENT_DATE) - INTERVAL '6 months'
                        GROUP BY TO_CHAR(transaction_time, 'Mon-YY')
                    )
                    SELECT m.month,
                        COALESCE(t.total_amount, 0) AS total_amount
                    FROM months m
                    LEFT JOIN transaction_totals t ON m.month = t.month
                    ORDER BY m.month_date;
                """
                # query = """
                # SELECT
                #     TO_CHAR(transaction_time, 'Mon-YY') AS month,
                #     SUM(amount) + SUM(total_amount) AS total_amount
                # FROM transactions
                # GROUP BY TO_CHAR(transaction_time, 'Mon-YY')
                # ORDER BY MIN(transaction_time);
                # """
                params = []

            cursor.execute(query, params)
            rows = cursor.fetchall()

            result = {"monthly": []}
            for row in rows:
                month_total_data = {
                    "month": row[0],
                    "totalAmount": row[1] if row[1] else 0,
                }
                monthly.append(month_total_data)

            result["monthly"] = monthly
            cache.set(key=cache_key, value=result, timeout=cache_timeout)

    return result


def settlement_report(duration):
    current_date = timezone.now()
    cache_key = f"settlement_record_report_{duration}"

    if (result := cache.get(cache_key)) is not None:
        return result

    result = {duration: []}

    with connections["etl_db"].cursor() as cursor:
        if duration == "daily":
            # Query for daily settlements
            cursor.execute(
                """
                    SELECT 
                DATE_TRUNC('day', s.createdate) AS period, 
                COUNT(sd.docno) AS settled_count, 
                COUNT(s.docno) AS total_count 
            FROM settlement_meb s
            LEFT JOIN settlement_detail sd ON s.docno = sd.docno
            WHERE s.createdate >=  %s 
            GROUP BY DATE_TRUNC('day', s.createdate)
            ORDER BY DATE_TRUNC('day', s.createdate);
            """,
                [current_date - relativedelta(days=7)],
            )

            daily_data = cursor.fetchall()
            for row in daily_data:
                day_data = {
                    "day": row[0].strftime("%Y-%m-%d"),
                    "totalSettleCount": row[1],
                    "totalUnSettleCount": row[2] - row[1],
                }
                result["daily"].append(day_data)

        elif duration == "weekly":
            # Query for weekly settlements
            cursor.execute(
                """
  

                SELECT 
                    DATE_TRUNC('week', s.createdate) AS period, 
                    COUNT(sd.docno) AS settled_count, 
                    COUNT(s.docno) AS total_count 
                FROM settlement_meb s
                LEFT JOIN settlement_detail sd ON s.docno = sd.docno
                WHERE s.createdate >= %s
                GROUP BY DATE_TRUNC('week', s.createdate)
                ORDER BY DATE_TRUNC('week', s.createdate);
            """,
                [current_date - relativedelta(weeks=7)],
            )

            weekly_data = cursor.fetchall()
            for row in weekly_data:
                week_data = {
                    "week": row[0].strftime("%Y-%m-%d"),
                    "totalSettleCount": row[1],
                    "totalUnSettleCount": row[2] - row[1],
                }
                result["weekly"].append(week_data)

        elif duration == "monthly":
            # Query for monthly settlements
            cursor.execute(
                """
                SELECT 
                    DATE_TRUNC('month', s.createdate) AS period, 
                    COUNT(sd.docno) AS settled_count, 
                    COUNT(s.docno) AS total_count 
                FROM settlement_meb s
                LEFT JOIN settlement_detail sd ON s.docno = sd.docno
                WHERE s.createdate >= %s
                GROUP BY DATE_TRUNC('month', s.createdate)
                ORDER BY DATE_TRUNC('month', s.createdate);
            """,
                [current_date - relativedelta(months=7)],
            )

            monthly_data = cursor.fetchall()
            for row in monthly_data:
                month_data = {
                    "month": row[0].strftime("%Y-%m"),
                    "totalSettleCount": row[1],
                    "totalUnSettleCount": row[2] - row[1],
                }
                result["monthly"].append(month_data)

        elif duration == "yearly":
            # Query for yearly settlements
            cursor.execute(
                """
                SELECT 
                    DATE_TRUNC('year', s.createdate) AS period, 
                    COUNT(sd.docno) AS settled_count, 
                    COUNT(s.docno) AS total_count 
                FROM settlement_meb s
                LEFT JOIN settlement_detail sd ON s.docno = sd.docno
                WHERE s.createdate >= %s
                GROUP BY DATE_TRUNC('year', s.createdate)
                ORDER BY DATE_TRUNC('year', s.createdate);
            """,
                [current_date - relativedelta(years=7)],
            )

            yearly_data = cursor.fetchall()
            for row in yearly_data:
                year_data = {
                    "year": row[0].strftime("%Y"),
                    "totalSettleCount": row[1],
                    "totalUnSettleCount": row[2] - row[1],
                }
                result["yearly"].append(year_data)

    cache.set(key=cache_key, value=result, timeout=cache_timeout)
    return result


def get_daily_transaction_trends(inst):
    current_date = timezone.now()
    inst_condition = (
        f"AND (issuer_institution_name = '{inst.bespokeCode}' OR issuer_institution_name = '{inst.tlaCode}')"
        if inst
        else ""
    )

    query = f"""
        SELECT
            to_char(current_date - interval '1 day' * i, 'DD Mon') AS day,
            COUNT(*) AS total_trend_count
        FROM generate_series(0, 6) AS i
        LEFT JOIN Transactions ON transaction_time BETWEEN current_date - interval '1 day' * i AND current_date - interval '1 day' * i + interval '1 day' - interval '1 second'
        WHERE 1=1
        {inst_condition}
        GROUP BY day
        ORDER BY day;
    """

    with connections["etl_db"].cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    return [{"day": row[0], "totalTrendCount": row[1]} for row in rows]


def get_weekly_transaction_trends(inst):
    current_date = timezone.now()
    inst_condition = (
        f"AND (issuer_institution_name = '{inst.bespokeCode}' OR issuer_institution_name = '{inst.tlaCode}')"
        if inst
        else ""
    )

    query = f"""
        SELECT
            'week ' || to_char(date_trunc('week', current_date) - interval '1 week' * i, 'IW') AS week,
            COUNT(*) AS total_trend_count
        FROM generate_series(0, 6) AS i
        LEFT JOIN Transactions ON transaction_time BETWEEN date_trunc('week', current_date) - interval '1 week' * i AND date_trunc('week', current_date) - interval '1 week' * i + interval '1 week' - interval '1 second'
        WHERE 1=1
        {inst_condition}
        GROUP BY week
        ORDER BY week;
    """

    with connections["etl_db"].cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    return [{"week": row[0], "totalTrendCount": row[1]} for row in rows]


def get_monthly_transaction_trends(inst):
    current_date = timezone.now()
    inst_condition = (
        f"AND (issuer_institution_name = '{inst.bespokeCode}' OR issuer_institution_name = '{inst.tlaCode}')"
        if inst
        else ""
    )

    query = f"""
        SELECT
            to_char(date_trunc('month', current_date) - interval '1 month' * i, 'Mon YYYY') AS month,
            COUNT(*) AS total_trend_count
        FROM generate_series(0, 6) AS i
        LEFT JOIN Transactions ON transaction_time BETWEEN date_trunc('month', current_date) - interval '1 month' * i AND date_trunc('month', current_date) - interval '1 month' * i + interval '1 month' - interval '1 second'
        WHERE 1=1
        {inst_condition}
        GROUP BY month
        ORDER BY month;
    """

    with connections["etl_db"].cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    return [{"month": row[0], "totalTrendCount": row[1]} for row in rows]


def get_yearly_transaction_trends(inst):
    current_date = timezone.now()
    inst_condition = (
        f"AND (issuer_institution_name = '{inst.bespokeCode}' OR issuer_institution_name = '{inst.tlaCode}')"
        if inst
        else ""
    )

    query = f"""
        SELECT
            to_char(date_trunc('year', current_date) - interval '1 year' * i, 'YYYY') AS year,
            COUNT(*) AS total_trend_count
        FROM generate_series(0, 6) AS i
        LEFT JOIN transactions ON transaction_time BETWEEN date_trunc('year', current_date) - interval '1 year' * i AND date_trunc('year', current_date) - interval '1 year' * i + interval '1 year' - interval '1 second'
        WHERE 1=1
        {inst_condition}
        GROUP BY year
        ORDER BY year;
    """

    with connections["etl_db"].cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    return [{"year": row[0], "totalTrendCount": row[1]} for row in rows]


def transaction_trends_report(user, inst_id, period):
    profile = UserDetail.objects.get(user=user)

    if profile.institution:
        inst = profile.institution
    elif inst_id is not None:
        inst = Institution.objects.get(id=inst_id)
    else:
        inst = None

    cache_key = f"{str(inst.name).replace(' ', '') if inst else 'global'}_transaction_trends_report_{period}"

    if cache.get(cache_key):
        result = cache.get(cache_key)
    else:
        if period == "daily":
            result = {"daily": get_daily_transaction_trends(inst)}
        elif period == "weekly":
            result = {"weekly": get_weekly_transaction_trends(inst)}
        elif period == "monthly":
            result = {"monthly": get_monthly_transaction_trends(inst)}
        elif period == "yearly":
            result = {"yearly": get_yearly_transaction_trends(inst)}
        else:
            result = {"daily": get_daily_transaction_trends(inst)}

        cache.set(key=cache_key, value=result, timeout=cache_timeout)

    return result


def get_date_range(duration, today):
    if duration == "" or duration is None:
        duration = "yesterday"
    if duration == "thisMonth":
        start_date, end_date = get_month_start_and_end_datetime(today)
    elif duration == "thisWeek":
        start_date, end_date = get_week_start_and_end_datetime(today)
    elif duration == "thisYear":
        start_date, end_date = get_year_start_and_end_datetime(today)
    else:
        yesterday = get_previous_date(today, 1)
        start_date, end_date = get_day_start_and_end_datetime(yesterday)
    return start_date, end_date


def local_to_foreign_transaction_report(user, inst_id, duration):
    user_profile = UserDetail.objects.get(user=user)
    today = timezone.now()
    start_date, end_date = get_date_range(duration, today)

    inst = None
    if user_profile.institution:
        inst = user_profile.institution
    elif inst_id:
        inst = Institution.objects.get(id=inst_id)

    cache_key = f"{str(inst.name).replace(' ', '') if inst else 'all'}_local_to_foreign_transaction_report_{duration}"
    result = cache.get(cache_key)

    if result is None:
        inst_condition = ""
        if inst:
            inst_condition = f"AND (issuer_institution_name = '{inst.bespokeCode}' OR issuer_institution_name = '{inst.tlaCode}')"

        query = f"""
            SELECT
                SUM(CASE WHEN issuer_country = '566' THEN total_amount ELSE 0 END) AS local_issuer_trans,
                SUM(CASE WHEN issuer_country IS NOT NULL AND issuer_country != '566' THEN total_amount ELSE 0 END) AS foreign_issuer_trans,
                SUM(CASE WHEN acquirer_country = '566' THEN total_amount ELSE 0 END) AS local_acquirer_trans,
                SUM(CASE WHEN acquirer_country IS NOT NULL AND acquirer_country != '566' THEN total_amount ELSE 0 END) AS foreign_acquirer_trans,
                SUM(CASE WHEN issuer_country = '566' THEN amount ELSE 0 END) AS local_issuer_trans_tla,
                SUM(CASE WHEN issuer_country IS NOT NULL AND issuer_country != '566' THEN amount ELSE 0 END) AS foreign_issuer_trans_tla,
                SUM(CASE WHEN acquirer_country = '566' THEN amount ELSE 0 END) AS local_acquirer_trans_tla,
                SUM(CASE WHEN acquirer_country IS NOT NULL AND acquirer_country != '566' THEN amount ELSE 0 END) AS foreign_acquirer_trans_tla
            FROM transactions
            WHERE transaction_time BETWEEN %s AND %s
            {inst_condition};
        """

        with connections["etl_db"].cursor() as cursor:
            cursor.execute(query, [start_date, end_date])
            row = cursor.fetchone()

        result = {
            "issuerTransaction": {
                "local": row[0] + row[4],
                "foreign": row[1] + row[5],
            },
            "aquirerTransaction": {
                "local": row[2] + row[6],
                "foreign": row[3] + row[7],
            },
        }
        cache.set(key=cache_key, value=result, timeout=cache_timeout)

    return result


def transaction_status(user, inst_id, duration):
    current_date = timezone.now()
    profile = UserDetail.objects.get(user=user)

    if profile.institution:
        inst = profile.institution
    elif inst_id is not None:
        inst = Institution.objects.get(id=inst_id)
    else:
        inst = None

    if inst is not None:
        cache_key = (
            f"{str(inst.name).replace(' ', '')}_transaction_status_report_{duration}"
        )
    else:
        cache_key = f"all_transaction_status_report_{duration}"

    result = cache.get(cache_key)

    if result is None:
        inst_condition = ""
        if inst:
            inst_condition = f"AND (issuer_institution_name = '{inst.bespokeCode}' OR issuer_institution_name = '{inst.tlaCode}')"

        result = {}

        if duration == "daily":
            start_date, end_date = get_day_start_and_end_datetime(
                current_date - relativedelta(days=1)
            )
            query = f"""
                WITH date_series AS (
                    SELECT generate_series(
                        CURRENT_DATE - interval '6 days',  
                        CURRENT_DATE,                       
                        interval '1 day'
                    )::date AS period_date
                ),
                transaction_totals AS (
                    SELECT
                        DATE(transaction_time) AS period_date,
                        SUM(CASE WHEN status_code = '00' THEN total_amount ELSE 0 END) AS success_amount,
                        SUM(CASE WHEN status_code = '03' THEN total_amount ELSE 0 END) AS failed_amount,
                        SUM(CASE WHEN transaction_status = '1' THEN amount ELSE 0 END) AS bespoke_success_amount,
                        SUM(CASE WHEN transaction_status != '1' THEN amount ELSE 0 END) AS bespoke_failed_amount
                    FROM transactions
                    WHERE transaction_time >= CURRENT_DATE - interval '7 days'  -- Last 7 days
                    AND transaction_time < CURRENT_DATE
                    {inst_condition}
                    GROUP BY DATE(transaction_time)
                )
                SELECT
                    ds.period_date,
                    COALESCE(tt.success_amount, 0) + COALESCE(tt.bespoke_success_amount, 0) AS success_amount,
                    COALESCE(tt.failed_amount, 0) + COALESCE(tt.bespoke_failed_amount, 0) AS failed_amount
                FROM date_series ds
                LEFT JOIN transaction_totals tt ON ds.period_date = tt.period_date
                ORDER BY ds.period_date;

            """
            with connections["etl_db"].cursor() as cursor:
                cursor.execute(query, [start_date, end_date])
                rows = cursor.fetchall()
            result["daily"] = []
            for row in rows:
                period_date, success_amount, failed_amount = row
                result["daily"].append(
                    {
                        "date": period_date.strftime("%d %b"),
                        "successAmount": success_amount,
                        "failedAmount": failed_amount,
                    }
                )

        elif duration == "weekly":
            start_date, end_date = get_week_start_and_end_datetime(current_date)
            query = f"""

                    WITH week_series AS (
                        SELECT 
                            DATE_TRUNC('week', CURRENT_DATE) - (s.a * interval '1 week') AS period_date
                        FROM 
                            generate_series(0, 6) AS s(a) -- Generate the last 7 weeks
                    ),
                    transaction_totals AS (
                        SELECT
                            DATE_TRUNC('week', transaction_time) AS period_date,
                            SUM(CASE WHEN status_code = '00' THEN total_amount ELSE 0 END) AS success_amount,
                            SUM(CASE WHEN status_code = '03' THEN total_amount ELSE 0 END) AS failed_amount,
                            SUM(CASE WHEN transaction_status = '1' THEN amount ELSE 0 END) AS bespoke_success_amount,
                            SUM(CASE WHEN transaction_status != '1' THEN amount ELSE 0 END) AS bespoke_failed_amount
                        FROM Transactions
                        WHERE transaction_time >= DATE_TRUNC('week', CURRENT_DATE) - interval '7 weeks'
                        AND transaction_time < CURRENT_DATE
                        GROUP BY DATE_TRUNC('week', transaction_time)
                    )
                    SELECT
                        ws.period_date,
                        COALESCE(tt.failed_amount, 0) + COALESCE(tt.bespoke_failed_amount, 0) AS failed_amount,
                        COALESCE(tt.success_amount, 0) + COALESCE(tt.bespoke_success_amount, 0) AS success_amount
                    FROM week_series ws
                    LEFT JOIN transaction_totals tt ON ws.period_date = tt.period_date
                    ORDER BY ws.period_date;

                    """
            # query = f"""
            #     SELECT
            #         DATE_TRUNC('week', transaction_time) AS period_date,
            #         SUM(CASE WHEN status_code = '00' THEN total_amount ELSE 0 END) AS success_amount,
            #         SUM(CASE WHEN status_code = '03' THEN total_amount ELSE 0 END) AS failed_amount
            #     FROM Transactions
            #     WHERE transaction_time >= %s AND transaction_time < %s {inst_condition}
            #     GROUP BY DATE_TRUNC('week', transaction_time)
            # """
            with connections["etl_db"].cursor() as cursor:
                cursor.execute(query, [start_date, end_date])
                rows = cursor.fetchall()
            result["weekly"] = []
            week_counter = 1
            for row in rows:
                period_date, success_amount, failed_amount = row
                result["weekly"].append(
                    {
                        "week": f"week {week_counter}",
                        "successAmount": success_amount,
                        "failedAmount": failed_amount,
                    }
                )
                week_counter += 1

        elif duration == "monthly":
            start_date, end_date = get_month_start_and_end_datetime(current_date)
            query = f"""
                WITH month_series AS (
                    SELECT 
                        DATE_TRUNC('month', CURRENT_DATE) - (s.a * interval '1 month') AS period_date
                    FROM 
                        generate_series(0, 6) AS s(a) -- Generate the last 7 months
                ),
                transaction_totals AS (
                    SELECT
                        DATE_TRUNC('month', transaction_time) AS period_date,
                        SUM(CASE WHEN status_code = '00' THEN total_amount ELSE 0 END) AS success_amount,
                        SUM(CASE WHEN status_code = '03' THEN total_amount ELSE 0 END) AS failed_amount,
                        SUM(CASE WHEN transaction_status = '1' THEN amount ELSE 0 END) AS bespoke_success_amount,
                        SUM(CASE WHEN transaction_status != '1' THEN amount ELSE 0 END) AS bespoke_failed_amount
                    FROM transactions
                    WHERE transaction_time >= DATE_TRUNC('month', CURRENT_DATE) - interval '7 months'
                    AND transaction_time < CURRENT_DATE
                    GROUP BY DATE_TRUNC('month', transaction_time)
                )
                SELECT
                    ms.period_date,
                    COALESCE(tt.failed_amount, 0) + COALESCE(tt.bespoke_failed_amount, 0) AS failed_amount,
                    COALESCE(tt.success_amount, 0) + COALESCE(tt.bespoke_success_amount, 0) AS success_amount
                FROM month_series ms
                LEFT JOIN transaction_totals tt ON ms.period_date = tt.period_date
                ORDER BY ms.period_date;
            """
            with connections["etl_db"].cursor() as cursor:
                cursor.execute(query, [start_date, end_date])
                rows = cursor.fetchall()
            result["monthly"] = []
            for row in rows:
                period_date, success_amount, failed_amount = row
                result["monthly"].append(
                    {
                        "month": period_date.strftime("%b"),
                        "successAmount": success_amount,
                        "failedAmount": failed_amount,
                    }
                )

        elif duration == "yearly":
            start_date, end_date = get_year_start_and_end_datetime(current_date)
            query = f"""
            WITH year_series AS (
                SELECT 
                    DATE_TRUNC('year', CURRENT_DATE) - (s.a * interval '1 year') AS period_date
                FROM 
                    generate_series(0, 6) AS s(a) -- Generate the last 7 years
            ),
            transaction_totals AS (
                SELECT
                    DATE_TRUNC('year', transaction_time) AS period_date,
                    SUM(CASE WHEN status_code = '00' THEN total_amount ELSE 0 END) AS success_amount,
                    SUM(CASE WHEN status_code = '03' THEN total_amount ELSE 0 END) AS failed_amount,
                    SUM(CASE WHEN transaction_status = '1' THEN total_amount ELSE 0 END) AS bespoke_success_amount,
                    SUM(CASE WHEN transaction_status != '1' THEN total_amount ELSE 0 END) AS bespoke_failed_amount
                FROM transactions
                WHERE transaction_time >= DATE_TRUNC('year', CURRENT_DATE) - interval '7 years'
                AND transaction_time < CURRENT_DATE
                GROUP BY DATE_TRUNC('year', transaction_time)
            )
            SELECT
                ys.period_date,
                COALESCE(tt.failed_amount, 0) + COALESCE(tt.bespoke_failed_amount, 0) AS failed_amount,
                COALESCE(tt.success_amount, 0) + COALESCE(tt.bespoke_success_amount, 0) AS success_amount
            FROM year_series ys
            LEFT JOIN transaction_totals tt ON ys.period_date = tt.period_date
            ORDER BY ys.period_date;
            """
            with connections["etl_db"].cursor() as cursor:
                cursor.execute(query, [start_date, end_date])
                rows = cursor.fetchall()
            result["yearly"] = []
            for row in rows:
                period_date, success_amount, failed_amount = row
                result["yearly"].append(
                    {
                        "year": period_date.strftime("%Y"),
                        "successAmount": success_amount,
                        "failedAmount": failed_amount,
                    }
                )

        cache.set(key=cache_key, value=result, timeout=cache_timeout)

    return result
