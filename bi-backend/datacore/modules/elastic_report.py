from dateutil.relativedelta import relativedelta
from django.core.cache import cache
from django.db.models import Count, Sum
from elasticsearch import helpers
from django.db import connections
from django.utils import timezone
from datetime import datetime, timedelta
from report.documents import (
    TransactionsDocument,
    SettlementMebDocument,
    SettlementDetailDocument,
    CardAccountDetailsIssuingDocument,
)
from django_elasticsearch_dsl.registries import registry
from django.conf import settings
from elasticsearch_dsl.query import Term
from elasticsearch_dsl import Q, Search
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.query import Term, Range
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
import requests

cache_timeout = settings.CACHE_TIMEOUT

ES_HOST = "http://196.46.20.44:9200"


def process_data(response, client, query):
    hits = response.hits
    total_hits = hits.total.value
    print(total_hits, "pppppp")

    # Initialize variables to store count and sum
    trans_count = 0
    trans_amount = 0
    if total_hits == 0:
        return 0, 0
    trans_amount += sum(hit[query] for hit in hits)

    # Increment the count by the number of hits in the current batch
    trans_count += len(hits)
    print(trans_count)
    while trans_count < total_hits:
        # Get the next batch of results using scroll API
        response = client.scroll(scroll_id=response["_scroll_id"], scroll="5m")

        # Get hits from the current batch
        hits = response["hits"]["hits"]

        # Calculate the sum of the total_amount field from the retrieved documents
        trans_amount += sum(hit["_source"][query] for hit in hits)

        # Increment the count by the number of hits in the current batch
        trans_count += len(hits)
    return trans_amount, trans_count


def process_dataw(hits, amount_field):
    total_amount = sum(hit["_source"].get(amount_field, 0) for hit in hits)
    total_count = len(hits)
    return total_amount, total_count


def process_data_with_addition_one_parameterw(
    hits, amount_field, parameter_value, parameter_name
):
    total_amount = sum(
        hit["_source"].get(amount_field, 0)
        for hit in hits
        if hit["_source"].get(parameter_name) == parameter_value
    )
    total_count = len(
        [hit for hit in hits if hit["_source"].get(parameter_name) == parameter_value]
    )
    return total_amount, total_count


def process_data_with_addition_one_parameter(
    response, client, query, param_value, param_name
):
    # Initialize variables to store count and sum
    trans_count = 0
    trans_amount = 0

    # Check if the response is in the expected format
    if "hits" in response:
        hits = response["hits"]
        total_hits = hits["total"]["value"]

        if total_hits == 0:
            return 0, 0

        # Calculate the sum of the total_amount field from the retrieved documents
        trans_amount += sum(
            hit["_source"][query]
            for hit in hits["hits"]
            if hit["_source"][param_name] == param_value
        )
        # #print(trans_amount, "kkkkkkkk")

        # Increment the count by the number of hits in the current batch
        trans_count += len(hits["hits"])

        while trans_count < total_hits:
            # Get the next batch of results using scroll API
            response = client.scroll(scroll_id=response["_scroll_id"], scroll="5m")

            # Check if the response is in the expected format
            if "hits" in response:
                hits = response["hits"]
                # Calculate the sum of the total_amount field from the retrieved documents
                trans_amount += sum(
                    hit["_source"][query]
                    for hit in hits["hits"]
                    if hit["_source"].get(param_name) == param_value
                )

                # Increment the count by the number of hits in the current batch
                trans_count += len(hits["hits"])
                # #print("dkdkjkjdkjjdkjkdj", trans_amount)
            else:
                # Break the loop if response is not in the expected format
                break

    return trans_amount, trans_count


def total_transaction_count_and_value(user, inst_id, duration):
    user_profile = UserDetail.objects.get(user=user)
    today = timezone.now()

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
        # Query all admin count within institution
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
    if result is not None:
        return result

    transaction_time_query = Q(
        "range",
        transaction_time={
            "gte": start_date.strftime("%Y-%m-%d"),
            "lte": end_date.strftime("%Y-%m-%d"),
        },
    )
    bespoke_query = (
        transaction_time_query & Q("term", issuer_institution_name=inst.bespokeCode)
        if inst
        else transaction_time_query
    )
    tla_query = (
        transaction_time_query & Q("term", issuer_institution_name=inst.tlaCode)
        if inst
        else transaction_time_query
    )
    # Execute queries against Elasticsearch
    client = connections.get_connection()
    filter_trans = TransactionsDocument.search()
    # ==================>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>================
    #  BESPOKE FILTER WITH ELASTIC SEARCH
    # ============================>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<
    bespoke_response = (
        filter_trans.filter(bespoke_query & Q("term", department="bespoke"))
        .params(scroll="5m", size=10000)
        .execute()
    )

    hits = bespoke_response.hits
    total_hits = hits.total.value

    # Initialize variables to store count and sum
    bes_trans_count = 0
    bes_trans_amount = 0

    # Calculate the sum of the total_amount field from the retrieved documents
    bes_trans_amount += sum(hit.total_amount for hit in hits)

    # Increment the count by the number of hits in the current batch
    bes_trans_count += len(hits)
    # #print(total_hits, bes_trans_amount, bes_trans_count, "bespoke")
    while bes_trans_count < total_hits:
        # Get the next batch of results using scroll API
        bespoke_response = client.scroll(
            scroll_id=bespoke_response["_scroll_id"], scroll="5m"
        )

        # Get hits from the current batch
        hits = bespoke_response["hits"]["hits"]

        # Calculate the sum of the total_amount field from the retrieved documents
        bes_trans_amount += sum(hit["_source"]["total_amount"] for hit in hits)

        # Increment the count by the number of hits in the current batch
        bes_trans_count += len(hits)
    # #print(total_hits, bes_trans_amount, bes_trans_count, "processing")

    client.clear_scroll(scroll_id=bespoke_response["_scroll_id"])
    # ==================>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>================
    #  CLEAR THE SCROLL RECORD
    # ============================>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<

    # ==================>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>================
    #  PROCESSING FILTER WITH ELASTIC SEARCH
    # ============================>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<
    tla_response = filter_trans.filter(tla_query & Q("term", department="processing"))
    tla_response = tla_response.params(scroll="5m", size=10000).execute()
    hits = tla_response.hits
    total_hits = hits.total.value

    # Initialize variables to store count and sum
    tla_trans_count = 0
    tla_trans_amount = 0

    # Calculate the sum of the total_amount field from the retrieved documents
    tla_trans_amount += sum(hit.amount for hit in hits)

    # Increment the count by the number of hits in the current batch
    tla_trans_count += len(hits)
    while tla_trans_count < total_hits:
        # Get the next batch of results using scroll API
        tla_response = client.scroll(scroll_id=tla_response["_scroll_id"], scroll="5m")

        # Get hits from the current batch
        hits = tla_response["hits"]["hits"]

        # Calculate the sum of the total_amount field from the retrieved documents
        tla_trans_amount += sum(hit["_source"]["amount"] for hit in hits)

        # Increment the count by the number of hits in the current batch
        tla_trans_count += len(hits)
    # #print(total_hits, tla_trans_amount, tla_trans_count, "pto")

    trans_count = bes_trans_count + tla_trans_count
    trans_amount = bes_trans_amount + tla_trans_amount

    result = {"count": trans_count, "amount": trans_amount}

    result.update({"overrallTotalTransactionAmount": trans_amount})
    cache.set(cache_key, result, timeout=cache_timeout)
    return result
    # ==================>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>================
    #  CLEARING THE PROCESSING
    # ============================>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<


# def transaction_status_per_channel(user, inst_id, channel):
#     yesterday = []
#     weekly = []
#     monthly = []
#     yearly = []
#     current_date = timezone.now()
#     profile = UserDetail.objects.get(user=user)
#     inst = (
#         profile.institution
#         if profile.institution
#         else Institution.objects.get(id=inst_id) if inst_id else None
#     )

#     cache_key = (
#         f"{inst.name.replace(' ', '')}_transactiion_status_per_channel_{channel}"
#         if inst
#         else f"all_transactiion_status_per_channel_{channel}"
#     )

#     client = connections.get_connection()
#     # Create a Search object
#     filter_trans = TransactionsDocument.search()

#     # Define the query to retrieve transaction status by channel
#     besp_q = Q("term", channel=channel)
#     if channel == "pos":
#         tla_q = Q("term", channel=2)
#     elif channel == "atm":
#         tla_q = Q("term", channel=1)
#     elif channel == "web":
#         tla_q = Q("term", channel=1)
#     else:
#         tla_q = Q("term", channel=0)

#     if inst:
#         tla_q &= Q("term", issuer_institution_name=inst.tlaCode)
#         besp_q &= Q("term", issuer_institution_name=inst.bespokeCode)
#     if cache.get(cache_key):
#         result = cache.get(cache_key)
#     else:
#         result = dict()

#         for delta in range(6, -1, -1):
#             yesterday_date = current_date - relativedelta(days=delta)
#             week_date = current_date - relativedelta(weeks=delta)
#             month_date = current_date - relativedelta(months=delta)
#             year_date = current_date - relativedelta(years=delta)
#             day_start, day_end = get_day_start_and_end_datetime(yesterday_date)
#             week_start, week_end = get_week_start_and_end_datetime(week_date)
#             month_start, month_end = get_month_start_and_end_datetime(month_date)
#             year_start, year_end = get_year_start_and_end_datetime(year_date)
#             trans_query_data = Q(
#                 "range",
#                 transaction_time={
#                     "gte": day_start.strftime("%Y-%m-%d"),
#                     "lte": day_end.strftime("%Y-%m-%d"),
#                 },
#             )
#             # Execute the search
#             # #print(trans_query_data)
#             tla_response = (
#                 filter_trans.query(
#                     tla_q & trans_query_data & Q("term", department="processing")
#                 )
#                 .params(scroll="5m", size=10000)
#                 .execute()
#             )
#             besp_response = (
#                 filter_trans.query(
#                     besp_q & trans_query_data & Q("term", department="processing")
#                 )
#                 .params(scroll="5m", size=10000)
#                 .execute()
#             )

#             besp_trans_amount, besp_trans_count = process_data(
#                 besp_response, client, "total_amount"
#             )
#             tla_trans_amount, tla_trans_count = process_data(
#                 tla_response, client, "amount"
#             )
#             # #print(besp_trans_amount)
#             bespoke_success_amount, _ = process_data_with_addition_one_parameter(
#                 besp_response, client, "total_amount", True, "transaction_status"
#             )
#             bespoke_failed_amount, _ = process_data_with_addition_one_parameter(
#                 besp_response, client, "total_amount", False, "transaction_status"
#             )
#             tla_success_amount, _ = process_data_with_addition_one_parameter(
#                 tla_response, client, "amount", 1, "transaction_status"
#             )
#             tla_failed_amount = tla_trans_amount - tla_success_amount
#             # #print(bespoke_failed_amount, bespoke_success_amount, "kkkkk")
#             yesterday_total_data = dict()
#             yesterday_total_data["day"] = day_start.strftime("%d %b")
#             yesterday_total_data["transactionAmount"] = (
#                 besp_trans_amount + tla_trans_amount
#             )
#             yesterday_total_data["totalCount"] = besp_trans_count + tla_trans_count
#             yesterday_total_data["totalSuccessAmount"] = (
#                 bespoke_success_amount + tla_success_amount
#             )
#             yesterday_total_data["totalFailedAmount"] = (
#                 bespoke_failed_amount + tla_failed_amount
#             )
#             yesterday_total_data["totalDeclinedAmount"] = 0
#             yesterday.append(yesterday_total_data)

#             #    WEEKLY FILTER
#             trans_query_data = Q(
#                 "range",
#                 transaction_time={
#                     "gte": week_start.strftime("%Y-%m-%d"),
#                     "lte": week_end.strftime("%Y-%m-%d"),
#                 },
#             )
#             # Execute the search
#             # #print(trans_query_data)
#             tla_response = (
#                 filter_trans.query(
#                     tla_q & trans_query_data & Q("term", department="processing")
#                 )
#                 .params(scroll="5m", size=10000)
#                 .execute()
#             )
#             besp_response = (
#                 filter_trans.query(
#                     besp_q & trans_query_data & Q("term", department="bespoke")
#                 )
#                 .params(scroll="5m", size=10000)
#                 .execute()
#             )

#             besp_trans_amount, besp_trans_count = process_data(
#                 besp_response, client, "total_amount"
#             )
#             tla_trans_amount, tla_trans_count = process_data(
#                 tla_response, client, "amount"
#             )
#             # #print(besp_trans_amount)
#             bespoke_success_amount, _ = process_data_with_addition_one_parameter(
#                 besp_response, client, "total_amount", True, "transaction_status"
#             )
#             bespoke_failed_amount, _ = process_data_with_addition_one_parameter(
#                 besp_response, client, "total_amount", False, "transaction_status"
#             )
#             tla_success_amount, _ = process_data_with_addition_one_parameter(
#                 tla_response, client, "amount", 1, "transaction_status"
#             )
#             tla_failed_amount = tla_trans_amount - tla_success_amount
#             # #print(bespoke_failed_amount, bespoke_success_amount, "kkkkk")
#             weekly_total_data = dict()
#             weekly_total_data["week"] = f"week {delta}"
#             weekly_total_data["transactionAmount"] = (
#                 besp_trans_amount + tla_trans_amount
#             )
#             weekly_total_data["totalCount"] = besp_trans_count + tla_trans_count
#             weekly_total_data["totalSuccessAmount"] = (
#                 bespoke_success_amount + tla_success_amount
#             )
#             weekly_total_data["totalFailedAmount"] = (
#                 bespoke_failed_amount + tla_failed_amount
#             )
#             weekly_total_data["totalDeclinedAmount"] = 0
#             weekly.append(weekly_total_data)

#             # MONTHLY FILTER
#             trans_query_data = Q(
#                 "range",
#                 transaction_time={
#                     "gte": month_start.strftime("%Y-%m-%d"),
#                     "lte": month_end.strftime("%Y-%m-%d"),
#                 },
#             )
#             # Execute the search
#             # #print(trans_query_data)
#             tla_response = (
#                 filter_trans.query(tla_q & trans_query_data)
#                 .params(scroll="5m", size=10000)
#                 .execute()
#             )
#             besp_response = (
#                 filter_trans.query(besp_q & trans_query_data)
#                 .params(scroll="5m", size=10000)
#                 .execute()
#             )

#             besp_trans_amount, besp_trans_count = process_data(
#                 besp_response, client, "total_amount"
#             )
#             tla_trans_amount, tla_trans_count = process_data(
#                 tla_response, client, "amount"
#             )
#             # #print(besp_trans_amount)
#             bespoke_success_amount, _ = process_data_with_addition_one_parameter(
#                 besp_response, client, "total_amount", True, "transaction_status"
#             )
#             bespoke_failed_amount, _ = process_data_with_addition_one_parameter(
#                 besp_response, client, "total_amount", False, "transaction_status"
#             )
#             tla_success_amount, _ = process_data_with_addition_one_parameter(
#                 tla_response, client, "amount", 1, "transaction_status"
#             )
#             tla_failed_amount = tla_trans_amount - tla_success_amount
#             # #print(bespoke_failed_amount, bespoke_success_amount, "kkkkk")
#             monthly_total_data = dict()
#             monthly_total_data["month"] = month_start.strftime("%b")
#             monthly_total_data["transactionAmount"] = (
#                 besp_trans_amount + tla_trans_amount
#             )
#             monthly_total_data["totalCount"] = besp_trans_count + tla_trans_count
#             monthly_total_data["totalSuccessAmount"] = (
#                 bespoke_success_amount + tla_success_amount
#             )
#             monthly_total_data["totalFailedAmount"] = (
#                 bespoke_failed_amount + tla_failed_amount
#             )
#             monthly_total_data["totalDeclinedAmount"] = 0
#             monthly.append(monthly_total_data)

#             # YEARLT FILTER
#             trans_query_data = Q(
#                 "range",
#                 transaction_time={
#                     "gte": year_start.strftime("%Y-%m-%d"),
#                     "lte": year_end.strftime("%Y-%m-%d"),
#                 },
#             )
#             # Execute the search
#             # #print(trans_query_data)
#             tla_response = (
#                 filter_trans.query(tla_q & trans_query_data)
#                 .params(scroll="5m", size=10000)
#                 .execute()
#             )
#             besp_response = (
#                 filter_trans.query(besp_q & trans_query_data)
#                 .params(scroll="5m", size=10000)
#                 .execute()
#             )

#             besp_trans_amount, besp_trans_count = process_data(
#                 besp_response, client, "total_amount"
#             )
#             tla_trans_amount, tla_trans_count = process_data(
#                 tla_response, client, "amount"
#             )
#             # #print(besp_trans_amount)
#             bespoke_success_amount, _ = process_data_with_addition_one_parameter(
#                 besp_response, client, "total_amount", True, "transaction_status"
#             )
#             bespoke_failed_amount, _ = process_data_with_addition_one_parameter(
#                 besp_response, client, "total_amount", False, "transaction_status"
#             )
#             tla_success_amount, _ = process_data_with_addition_one_parameter(
#                 tla_response, client, "amount", 1, "transaction_status"
#             )
#             tla_failed_amount = tla_trans_amount - tla_success_amount
#             # #print(bespoke_failed_amount, bespoke_success_amount, "kkkkk")
#             yearly_total_data = dict()
#             yearly_total_data["year"] = year_start.strftime("%Y")
#             yearly_total_data["transactionAmount"] = (
#                 besp_trans_amount + tla_trans_amount
#             )
#             yearly_total_data["totalCount"] = besp_trans_count + tla_trans_count
#             yearly_total_data["totalSuccessAmount"] = (
#                 bespoke_success_amount + tla_success_amount
#             )
#             yearly_total_data["totalFailedAmount"] = (
#                 bespoke_failed_amount + tla_failed_amount
#             )
#             yearly_total_data["totalDeclinedAmount"] = 0
#             yearly.append(yearly_total_data)
#         result["daily"] = yesterday
#         result["weekly"] = weekly
#         result["monthly"] = monthly
#         result["yearly"] = yearly
#         cache.set(key=cache_key, value=result, timeout=cache_timeout)
#     return result


def fetch_hits_from_es(query_body):
    try:
        response = requests.post(
            f"{ES_HOST}/transaction_index/_search?scroll=5m", json=query_body
        ).json()
        print(response, "ooooo")
        if "hits" in response and "hits" in response["hits"]:
            return response["hits"]["hits"]
        else:
            return []
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return []


def transaction_status_per_channel(user, inst_id, channel):
    yesterday = []
    weekly = []
    monthly = []
    yearly = []
    current_date = timezone.now()
    profile = UserDetail.objects.get(user=user)
    inst = (
        profile.institution
        if profile.institution
        else Institution.objects.get(id=inst_id) if inst_id else None
    )

    cache_key = (
        f"{inst.name.replace(' ', '')}_transactiion_status_per_channel_{channel}"
        if inst
        else f"all_transactiion_status_per_channel_{channel}"
    )

    # if not cache.get(cache_key):
    #     return cache.get(cache_key)

    result = dict()

    for delta in range(6, -1, -1):
        yesterday_date = current_date - relativedelta(days=delta)
        week_date = current_date - relativedelta(weeks=delta)
        month_date = current_date - relativedelta(months=delta)
        year_date = current_date - relativedelta(years=delta)
        day_start, day_end = get_day_start_and_end_datetime(yesterday_date)
        week_start, week_end = get_week_start_and_end_datetime(week_date)
        month_start, month_end = get_month_start_and_end_datetime(month_date)
        year_start, year_end = get_year_start_and_end_datetime(year_date)

        # Define the queries
        trans_query_data = {
            "range": {
                "transaction_time": {
                    "gte": day_start.strftime("%Y-%m-%d"),
                    "lte": day_end.strftime("%Y-%m-%d"),
                }
            }
        }

        # Create filter queries based on channel and institution
        channel_query = {"term": {"channel": channel}}
        if channel == "pos":
            channel_query = {"term": {"channel": 2}}
        elif channel == "atm" or channel == "web":
            channel_query = {"term": {"channel": 1}}
        else:
            channel_query = {"term": {"channel": 0}}

        tla_query = channel_query.copy()
        besp_query = channel_query.copy()
        if inst:
            tla_query["term"]["issuer_institution_name"] = inst.tlaCode
            besp_query["term"]["issuer_institution_name"] = inst.bespokeCode

        department_query = {"term": {"department": "processing"}}

        # Build the request payload for Elasticsearch
        query_body = {
            "query": {
                "bool": {"must": [tla_query, trans_query_data, department_query]}
            },
            "size": 10000,
        }

        # Execute the query for TLA
        tla_hits = fetch_hits_from_es(query_body)
        print(tla_hits)

        # Execute the query for BESPOKE
        department_query = {"term": {"department": "bespoke"}}
        query_body["query"]["bool"]["must"][2] = department_query
        besp_hits = fetch_hits_from_es(query_body)

        # Process the responses
        besp_trans_amount, besp_trans_count = process_dataw(besp_hits, "total_amount")
        tla_trans_amount, tla_trans_count = process_dataw(tla_hits, "amount")
        bespoke_success_amount, _ = process_data_with_addition_one_parameterw(
            besp_hits, "total_amount", True, "transaction_status"
        )
        bespoke_failed_amount, _ = process_data_with_addition_one_parameterw(
            besp_hits, "total_amount", False, "transaction_status"
        )
        tla_success_amount, _ = process_data_with_addition_one_parameterw(
            tla_hits, "amount", 1, "transaction_status"
        )
        tla_failed_amount = tla_trans_amount - tla_success_amount

        # Compile the results for each time period
        yesterday_total_data = {
            "day": day_start.strftime("%d %b"),
            "transactionAmount": besp_trans_amount + tla_trans_amount,
            "totalCount": besp_trans_count + tla_trans_count,
            "totalSuccessAmount": bespoke_success_amount + tla_success_amount,
            "totalFailedAmount": bespoke_failed_amount + tla_failed_amount,
            "totalDeclinedAmount": 0,
        }
        yesterday.append(yesterday_total_data)
        trans_query_data["range"]["transaction_time"]["gte"] = week_start.strftime(
            "%Y-%m-%d"
        )
        trans_query_data["range"]["transaction_time"]["lte"] = week_end.strftime(
            "%Y-%m-%d"
        )

        # Execute the query for TLA
        query_body["query"]["bool"]["must"][1] = trans_query_data
        query_body["query"]["bool"]["must"][2] = {"term": {"department": "processing"}}
        tla_hits = fetch_hits_from_es(query_body)

        # Execute the query for BESPOKE
        query_body["query"]["bool"]["must"][2] = {"term": {"department": "bespoke"}}
        besp_hits = fetch_hits_from_es(query_body)

        # Process the responses
        besp_trans_amount, besp_trans_count = process_dataw(besp_hits, "total_amount")
        tla_trans_amount, tla_trans_count = process_dataw(tla_hits, "amount")
        bespoke_success_amount, _ = process_data_with_addition_one_parameterw(
            besp_hits, "total_amount", True, "transaction_status"
        )
        bespoke_failed_amount, _ = process_data_with_addition_one_parameterw(
            besp_hits, "total_amount", False, "transaction_status"
        )
        tla_success_amount, _ = process_data_with_addition_one_parameterw(
            tla_hits, "amount", 1, "transaction_status"
        )
        tla_failed_amount = tla_trans_amount - tla_success_amount

        weekly_total_data = {
            "week": f"week {delta}",
            "transactionAmount": besp_trans_amount + tla_trans_amount,
            "totalCount": besp_trans_count + tla_trans_count,
            "totalSuccessAmount": bespoke_success_amount + tla_success_amount,
            "totalFailedAmount": bespoke_failed_amount + tla_failed_amount,
            "totalDeclinedAmount": 0,
        }
        weekly.append(weekly_total_data)

        # Compile the results for monthly data
        trans_query_data["range"]["transaction_time"]["gte"] = month_start.strftime(
            "%Y-%m-%d"
        )
        trans_query_data["range"]["transaction_time"]["lte"] = month_end.strftime(
            "%Y-%m-%d"
        )

        # Execute the query for TLA
        query_body["query"]["bool"]["must"][1] = trans_query_data
        query_body["query"]["bool"]["must"][2] = {"term": {"department": "processing"}}
        tla_hits = fetch_hits_from_es(query_body)

        # Execute the query for BESPOKE
        query_body["query"]["bool"]["must"][2] = {"term": {"department": "bespoke"}}
        besp_hits = fetch_hits_from_es(query_body)

        # Process the responses
        besp_trans_amount, besp_trans_count = process_dataw(besp_hits, "total_amount")
        tla_trans_amount, tla_trans_count = process_dataw(tla_hits, "amount")

        bespoke_success_amount, _ = process_data_with_addition_one_parameterw(
            besp_hits, "total_amount", True, "transaction_status"
        )
        bespoke_failed_amount, _ = process_data_with_addition_one_parameterw(
            besp_hits, "total_amount", False, "transaction_status"
        )
        tla_success_amount, _ = process_data_with_addition_one_parameterw(
            tla_hits, "amount", 1, "transaction_status"
        )
        tla_failed_amount = tla_trans_amount - tla_success_amount

        monthly_total_data = {
            "month": month_start.strftime("%b"),
            "transactionAmount": besp_trans_amount + tla_trans_amount,
            "totalCount": besp_trans_count + tla_trans_count,
            "totalSuccessAmount": bespoke_success_amount + tla_success_amount,
            "totalFailedAmount": bespoke_failed_amount + tla_failed_amount,
            "totalDeclinedAmount": 0,
        }
        monthly.append(monthly_total_data)

        # Compile the results for yearly data
        trans_query_data["range"]["transaction_time"]["gte"] = year_start.strftime(
            "%Y-%m-%d"
        )
        trans_query_data["range"]["transaction_time"]["lte"] = year_end.strftime(
            "%Y-%m-%d"
        )

        # Execute the query for TLA
        query_body["query"]["bool"]["must"][1] = trans_query_data
        query_body["query"]["bool"]["must"][2] = {"term": {"department": "processing"}}
        tla_hits = fetch_hits_from_es(query_body)

        # Execute the query for BESPOKE
        query_body["query"]["bool"]["must"][2] = {"term": {"department": "bespoke"}}
        besp_hits = fetch_hits_from_es(query_body)

        # Process the responses
        besp_trans_amount, besp_trans_count = process_dataw(besp_hits, "total_amount")
        tla_trans_amount, tla_trans_count = process_dataw(tla_hits, "amount")
        bespoke_success_amount, _ = process_data_with_addition_one_parameterw(
            besp_hits, "total_amount", True, "transaction_status"
        )
        bespoke_failed_amount, _ = process_data_with_addition_one_parameterw(
            besp_hits, "total_amount", False, "transaction_status"
        )
        tla_success_amount, _ = process_data_with_addition_one_parameterw(
            tla_hits, "amount", 1, "transaction_status"
        )
        tla_failed_amount = tla_trans_amount - tla_success_amount

        yearly_total_data = {
            "year": year_start.strftime("%Y"),
            "transactionAmount": besp_trans_amount + tla_trans_amount,
            "totalCount": besp_trans_count + tla_trans_count,
            "totalSuccessAmount": bespoke_success_amount + tla_success_amount,
            "totalFailedAmount": bespoke_failed_amount + tla_failed_amount,
            "totalDeclinedAmount": 0,
        }
        yearly.append(yearly_total_data)
    result["daily"] = yesterday
    result["weekly"] = weekly
    result["monthly"] = monthly
    result["yearly"] = yearly

    cache.set(key=cache_key, value=result, timeout=cache_timeout)
    return result


def local_to_foreign_transaction_report(user, inst_id, duration):
    user_profile = UserDetail.objects.get(user=user)
    today = timezone.now()

    if duration == "" or None:
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
    client = connections.get_connection()
    # Create a Search object
    trans_s = TransactionsDocument.search()
    if inst is not None:
        cache_key = f"{str(inst.name).replace(' ', '')}_local_to_foreign_transaction_reports_{duration}"
    else:
        cache_key = f"all_local_to_foreign_transaction_reports_{duration}"
    result = cache.get(cache_key)

    if result is None:
        trans_query_data = Q(
            "range",
            transaction_time={
                "gte": start_date.strftime("%Y-%m-%d"),
                "lte": end_date.strftime("%Y-%m-%d"),
            },
        )

        if inst is not None:
            bespoke_isnt = (
                Q("term", issuer_institution_name=inst.bespokeCode) & trans_query_data
            )
            tla_isnt = (
                Q("term", issuer_institution_name=inst.tlaCode) & trans_query_data
            )
        else:
            bespoke_isnt = Q("term", department="bespoke") & trans_query_data
            tla_isnt = Q("term", department="processing") & trans_query_data
        bespoke_local_issuer_trans, _ = process_data(
            trans_s.query(bespoke_isnt & Q("term", issuer_country="566"))
            .params(scroll="5m", size=10000)
            .execute(),
            client,
            "total_amount",
        )
        tla_local_issuer_trans, _ = process_data(
            trans_s.query(tla_isnt & Q("term", issuer_country="566"))
            .params(scroll="5m", size=10000)
            .execute(),
            client,
            "amount",
        )

        bespoke_foreign_issuer_trans = 0
        tla_foreign_issuer_trans, _ = process_data(
            trans_s.query(tla_isnt & ~Q("term", issuer_country="566"))
            .params(scroll="5m", size=10000)
            .execute(),
            client,
            "amount",
        )
        bespoke_local_aquirer_trans, _ = process_data(
            trans_s.query(bespoke_isnt & Q("term", acquirer_country="566"))
            .params(scroll="5m", size=10000)
            .execute(),
            client,
            "total_amount",
        )
        tla_local_aquirer_trans, _ = process_data(
            trans_s.query(tla_isnt & Q("term", acquirer_country="566"))
            .params(scroll="5m", size=10000)
            .execute(),
            client,
            "amount",
        )
        bespoke_foreign_aquirer_trans = 0
        tla_foreign_aquirer_trans, _ = process_data(
            trans_s.query(tla_isnt & ~Q("term", acquirer_country="566"))
            .params(scroll="5m", size=10000)
            .execute(),
            client,
            "amount",
        )
        # ##print("ppppppppp",tla_foreign_aquirer_trans,bespoke_foreign_issuer_trans)
        # #print(
        #     "bespoke_local_issuer_trans",
        #     bespoke_local_issuer_trans,
        #     "tla_local_issuer_trans",
        #     tla_local_issuer_trans,
        #     "bespoke_local_aquirer_trans",
        #     bespoke_local_aquirer_trans,
        #     "tla_local_aquirer_trans",
        #     tla_local_aquirer_trans,
        # )
        result = {
            "issuerTransaction": {
                "local": bespoke_local_issuer_trans + tla_local_issuer_trans,
                "foreign": bespoke_foreign_issuer_trans + tla_foreign_issuer_trans,
            },
            "aquirerTransaction": {
                "local": bespoke_local_aquirer_trans + tla_local_aquirer_trans,
                "foreign": bespoke_foreign_aquirer_trans + tla_foreign_aquirer_trans,
            },
        }
        cache.set(key=cache_key, value=result, timeout=cache_timeout)
        # connections['etl_db'].close()

    return result


def transaction_status(user, inst_id, duration):
    current_date = timezone.now()
    profile = UserDetail.objects.get(user=user)
    yesterday = []
    monthly = []
    yearly = []
    weekly = []

    if profile.institution:
        inst = profile.institution
    elif inst_id is not None:
        inst = Institution.objects.get(id=inst_id)
    else:
        inst = None

    if inst is not None:
        cache_key = f"{str(inst.name).replace(' ', '')}_{duration}_transaction_status_returns_reports"
    else:
        cache_key = f"all_transaction_status_returns_reports_{duration}"

    if cache.get(cache_key):
        result = cache.get(cache_key)
    else:
        result = dict()
        transaction_s = TransactionsDocument.search()
        week = 0
        query = Q()
        client = connections.get_connection()

        if inst:
            query &= Q("term", issuer_institution_name=inst.bespokeCode) | Q(
                "term", issuer_institution_name=inst.tlaCode
            )
        for delta in range(6, -1, -1):
            yesterday_date = current_date - relativedelta(days=delta)
            week_date = current_date - relativedelta(weeks=delta)
            month_date = current_date - relativedelta(months=delta)
            year_date = current_date - relativedelta(years=delta)

            if duration == "thisWeek":
                week_start, week_end = get_week_start_and_end_datetime(week_date)

                trans_date = Q(
                    "range",
                    transaction_time={
                        "gte": week_start.strftime("%Y-%m-%d"),
                        "lte": week_end.strftime("%Y-%m-%d"),
                    },
                )
                bespoke_success_amount, _ = process_data(
                    transaction_s.query(
                        query
                        & trans_date
                        & Q("term", status_code="00")
                        & Q("term", department="bespoke")
                    )
                    .params(scroll="5m", size=10000)
                    .execute(),
                    client,
                    "total_amount",
                )
                bespoke_failed_amount, _ = process_data(
                    transaction_s.query(
                        query
                        & trans_date
                        & Q("term", status_code="03")
                        & Q("term", department="bespoke")
                    )
                    .params(scroll="5m", size=10000)
                    .execute(),
                    client,
                    "total_amount",
                )
                tla_suc_amt, _ = process_data(
                    transaction_s.query(
                        query
                        & trans_date
                        & Q("term", transaction_status="1")
                        & Q("term", department="processing")
                    )
                    .params(scroll="5m", size=10000)
                    .execute(),
                    client,
                    "amount",
                )
                tla_fail_amt, _ = process_data(
                    transaction_s.query(
                        query
                        & trans_date
                        & Q("term", transaction_status="1")
                        & Q("term", department="processing")
                    )
                    .params(scroll="5m", size=10000)
                    .execute(),
                    client,
                    "amount",
                )
                week = week + 1
                week_total_data = dict()
                # f'{week_start.strftime("%d %b")} - {week_end.strftime("%d %b")}'
                week_total_data["week"] = f"week {week}"
                week_total_data["successAmount"] = bespoke_success_amount + tla_suc_amt
                week_total_data["failedAmount"] = bespoke_failed_amount + tla_fail_amt

                weekly.append(week_total_data)
            elif duration == "thisMonth":
                month_start, month_end = get_month_start_and_end_datetime(month_date)

                trans_date = Q(
                    "range",
                    transaction_time={
                        "gte": month_start.strftime("%Y-%m-%d"),
                        "lte": month_end.strftime("%Y-%m-%d"),
                    },
                )
                bespoke_success_amount, _ = process_data(
                    transaction_s.query(
                        query
                        & trans_date
                        & Q("term", status_code="00")
                        & Q("term", department="bespoke")
                    )
                    .params(scroll="5m", size=10000)
                    .execute(),
                    client,
                    "total_amount",
                )
                bespoke_failed_amount, _ = process_data(
                    transaction_s.query(
                        query
                        & trans_date
                        & Q("term", status_code="03")
                        & Q("term", department="bespoke")
                    )
                    .params(scroll="5m", size=10000)
                    .execute(),
                    client,
                    "total_amount",
                )
                tla_suc_amt, _ = process_data(
                    transaction_s.query(
                        query
                        & trans_date
                        & Q("term", transaction_status="1")
                        & Q("term", department="processing")
                    )
                    .params(scroll="5m", size=10000)
                    .execute(),
                    client,
                    "amount",
                )
                tla_fail_amt, _ = process_data(
                    transaction_s.query(
                        query
                        & trans_date
                        & Q("term", transaction_status="1")
                        & Q("term", department="processing")
                    )
                    .params(scroll="5m", size=10000)
                    .execute(),
                    client,
                    "amount",
                )

                month_total_data = dict()
                month_total_data["month"] = month_start.strftime("%b")
                month_total_data["successAmount"] = bespoke_success_amount + tla_suc_amt
                month_total_data["failedAmount"] = bespoke_failed_amount + tla_fail_amt

                monthly.append(month_total_data)
            elif duration == "thisYear":
                year_start, year_end = get_year_start_and_end_datetime(year_date)

                trans_date = Q(
                    "range",
                    transaction_time={
                        "gte": year_start.strftime("%Y-%m-%d"),
                        "lte": year_end.strftime("%Y-%m-%d"),
                    },
                )
                bespoke_success_amount, _ = process_data(
                    transaction_s.query(
                        query
                        & trans_date
                        & Q("term", status_code="00")
                        & Q("term", department="bespoke")
                    )
                    .params(scroll="5m", size=10000)
                    .execute(),
                    client,
                    "total_amount",
                )
                bespoke_failed_amount, _ = process_data(
                    transaction_s.query(
                        query
                        & trans_date
                        & Q("term", status_code="03")
                        & Q("term", department="bespoke")
                    )
                    .params(scroll="5m", size=10000)
                    .execute(),
                    client,
                    "total_amount",
                )
                tla_suc_amt, _ = process_data(
                    transaction_s.query(
                        query
                        & trans_date
                        & Q("term", transaction_status="1")
                        & Q("term", department="processing")
                    )
                    .params(scroll="5m", size=10000)
                    .execute(),
                    client,
                    "amount",
                )
                tla_fail_amt, _ = process_data(
                    transaction_s.query(
                        query
                        & trans_date
                        & Q("term", transaction_status="1")
                        & Q("term", department="processing")
                    )
                    .params(scroll="5m", size=10000)
                    .execute(),
                    client,
                    "amount",
                )

                year_total_data = dict()
                year_total_data["year"] = year_start.strftime("%Y")
                year_total_data["successAmount"] = bespoke_success_amount + tla_suc_amt
                year_total_data["failedAmount"] = bespoke_failed_amount + tla_fail_amt

                yearly.append(year_total_data)
            else:
                day_start, day_end = get_day_start_and_end_datetime(yesterday_date)
                trans_date = Q(
                    "range",
                    transaction_time={
                        "gte": day_end.strftime("%Y-%m-%d"),
                        "lte": day_end.strftime("%Y-%m-%d"),
                    },
                )
                bespoke_success_amount, _ = process_data(
                    transaction_s.query(
                        query
                        & trans_date
                        & Q("term", status_code="00")
                        & Q("term", department="bespoke")
                    )
                    .params(scroll="5m", size=10000)
                    .execute(),
                    client,
                    "total_amount",
                )
                bespoke_failed_amount, _ = process_data(
                    transaction_s.query(
                        query
                        & trans_date
                        & Q("term", status_code="03")
                        & Q("term", department="bespoke")
                    )
                    .params(scroll="5m", size=10000)
                    .execute(),
                    client,
                    "total_amount",
                )
                tla_suc_amt, _ = process_data(
                    transaction_s.query(
                        query
                        & trans_date
                        & Q("term", transaction_status="1")
                        & Q("term", department="processing")
                    )
                    .params(scroll="5m", size=10000)
                    .execute(),
                    client,
                    "amount",
                )
                tla_fail_amt, _ = process_data(
                    transaction_s.query(
                        query
                        & trans_date
                        & Q("term", transaction_status="1")
                        & Q("term", department="processing")
                    )
                    .params(scroll="5m", size=10000)
                    .execute(),
                    client,
                    "amount",
                )

                yesterday_total_data = dict()
                yesterday_total_data["day"] = day_end.strftime("%d %b")
                yesterday_total_data["successAmount"] = (
                    bespoke_success_amount + tla_suc_amt
                )
                yesterday_total_data["failedAmount"] = (
                    bespoke_failed_amount + tla_fail_amt
                )

                yesterday.append(yesterday_total_data)
            # #print(yesterday, weekly, monthly, yearly)

        result["yesterday"] = yesterday
        result["weekly"] = weekly
        result["monthly"] = monthly
        result["yearly"] = yearly
        cache.set(key=cache_key, value=result, timeout=cache_timeout)

    return result


def transaction_trends_report(user, inst_id):
    current_date = timezone.now()
    profile = UserDetail.objects.get(user=user)
    yesterday = []
    monthly = []
    yearly = []
    weekly = []

    if profile.institution:
        inst = profile.institution
    elif inst_id is not None:
        inst = Institution.objects.get(id=inst_id)
    else:
        inst = None

    if inst is not None:
        cache_key = f"{str(inst.name).replace(' ', '')}_transaction_trend_reports"
    else:
        cache_key = "all_transaction_trending_reports"

    if cache.get(cache_key):

        result = cache.get(cache_key)
        return result
    else:
        result = dict()
        week = 0
        # Create a Search object
        tran_s = TransactionsDocument.search()

        for delta in range(6, -1, -1):
            yesterday_date = current_date - relativedelta(days=delta)
            week_date = current_date - relativedelta(weeks=delta)
            month_date = current_date - relativedelta(months=delta)
            year_date = current_date - relativedelta(years=delta)
            day_start, day_end = get_day_start_and_end_datetime(yesterday_date)
            week_start, week_end = get_week_start_and_end_datetime(week_date)
            month_start, month_end = get_month_start_and_end_datetime(month_date)
            year_start, year_end = get_year_start_and_end_datetime(year_date)

            query = Q()
            if inst:
                tla_q = Q("term", issuer_institution_name=inst.tlaCode)
                besp_q = Q("term", issuer_institution_name=inst.bespokeCode)
            else:
                tla_q = Q("term", department="processing")
                besp_q = Q("term", department="bespoke")

            # if inst:
            #     query &= Q(issuer_institution_name=inst.bespokeCode) | Q(issuer_institution_name=inst.tlaCode)
            # query &= Q(
            #     "range",
            #     transaction_time={
            #         "gte": day_start.strftime("%Y-%m-%d"),
            #         "lte": day_end.strftime("%Y-%m-%d"),
            #     },
            # )
            # tla_response = tla_s.query(tla_q & query).params(scroll='5m', size=10000).execute()
            # besp_response = besp_s.query(besp_q & query ).params(scroll='5m', size=10000).execute()

            # besp_trans_amount,besp_trans_count= process_data(besp_response,client,"total_amount")
            # tla_trans_amount,tla_trans_count = process_data(tla_response,client,"amount")
            tla_trans_count = (
                tran_s.query(
                    tla_q
                    & Q(
                        "range",
                        transaction_time={
                            "gte": day_end.strftime("%Y-%m-%d"),
                            "lte": day_end.strftime("%Y-%m-%d"),
                        },
                    )
                )
                .params(scroll="5m", size=10000)
                .execute()
                .hits.total.value
            )

            besp_trans_count = (
                tran_s.query(
                    besp_q
                    & Q(
                        "range",
                        transaction_time={
                            "gte": day_end.strftime("%Y-%m-%d"),
                            "lte": day_end.strftime("%Y-%m-%d"),
                        },
                    )
                )
                .params(scroll="5m", size=10000)
                .execute()
                .hits.total.value
            )

            # #print(day_start, day_end)
            # #print(besp_trans_count, tla_trans_count)
            total_count = tla_trans_count + besp_trans_count
            yesterday_total_data = dict()
            yesterday_total_data["day"] = day_end.strftime("%d %b")
            yesterday_total_data["totalTrendCount"] = total_count

            yesterday.append(yesterday_total_data)

            tla_trans_count = (
                tran_s.query(
                    tla_q
                    & Q(
                        "range",
                        transaction_time={
                            "gte": week_start.strftime("%Y-%m-%d"),
                            "lte": week_end.strftime("%Y-%m-%d"),
                        },
                    )
                )
                .params(scroll="5m", size=10000)
                .execute()
                .hits.total.value
            )

            besp_trans_count = (
                tran_s.query(
                    besp_q
                    & Q(
                        "range",
                        transaction_time={
                            "gte": week_start.strftime("%Y-%m-%d"),
                            "lte": week_end.strftime("%Y-%m-%d"),
                        },
                    )
                )
                .params(scroll="5m", size=10000)
                .execute()
                .hits.total.value
            )

            # print(week_start, week_end, besp_trans_count, tla_trans_count)
            total_count = tla_trans_count + besp_trans_count
            week = week + 1
            week_total_data = dict()
            # f"{week_start.strftime('%d %b')} - {week_end.strftime('%d %b')}"
            week_total_data["week"] = f"week {week}"
            week_total_data["totalTrendCount"] = total_count

            weekly.append(week_total_data)

            query &= Q(
                "range",
                transaction_time={
                    "gte": month_start.strftime("%Y-%m-%d"),
                    "lte": month_end.strftime("%Y-%m-%d"),
                },
            )
            tla_trans_count = (
                tran_s.query(
                    tla_q
                    & Q(
                        "range",
                        transaction_time={
                            "gte": month_start.strftime("%Y-%m-%d"),
                            "lte": month_end.strftime("%Y-%m-%d"),
                        },
                    )
                )
                .params(scroll="5m", size=10000)
                .execute()
                .hits.total.value
            )

            besp_trans_count = (
                (
                    tran_s.query(
                        besp_q
                        & Q(
                            "range",
                            transaction_time={
                                "gte": month_start.strftime("%Y-%m-%d"),
                                "lte": month_end.strftime("%Y-%m-%d"),
                            },
                        )
                    )
                )
                .params(scroll="5m", size=10000)
                .execute()
                .hits.total.value
            )
            total_count = tla_trans_count + besp_trans_count

            month_total_data = dict()
            month_total_data["month"] = month_start.strftime("%b")
            month_total_data["totalTrendCount"] = total_count

            monthly.append(month_total_data)

            query &= Q(
                "range",
                transaction_time={
                    "gte": year_start.strftime("%Y-%m-%d"),
                    "lte": year_end.strftime("%Y-%m-%d"),
                },
            )
            tla_trans_count = (
                tran_s.query(
                    tla_q
                    & Q(
                        "range",
                        transaction_time={
                            "gte": year_start.strftime("%Y-%m-%d"),
                            "lte": year_end.strftime("%Y-%m-%d"),
                        },
                    )
                )
                .params(scroll="5m", size=10000)
                .execute()
                .hits.total.value
            )

            besp_trans_count = (
                tran_s.query(
                    besp_q
                    & Q(
                        "range",
                        transaction_time={
                            "gte": year_start.strftime("%Y-%m-%d"),
                            "lte": year_end.strftime("%Y-%m-%d"),
                        },
                    )
                )
                .params(scroll="5m", size=10000)
                .execute()
                .hits.total.value
            )
            total_count = tla_trans_count + besp_trans_count

            year_total_data = dict()
            year_total_data["year"] = year_start.strftime("%Y")
            year_total_data["totalTrendCount"] = total_count

            yearly.append(year_total_data)

        result["daily"] = yesterday
        result["weekly"] = weekly
        result["monthly"] = monthly
        result["yearly"] = yearly
        cache.set(key=cache_key, value=result, timeout=cache_timeout)

    return result


def transaction_by_channel_report(user, inst_id, duration):
    today = datetime.now()
    profile = UserDetail.objects.get(user=user)
    if duration == "" or duration is None:
        duration = "yesterday"

    if duration == "thisMonth":
        start_date, end_date = get_month_start_and_end_datetime(today)
    elif duration == "thisWeek":
        start_date, end_date = get_week_start_and_end_datetime(today)
    elif duration == "thisYear":
        start_date, end_date = get_year_start_and_end_datetime(today)
    else:
        yesterday = today - timedelta(days=1)
        start_date, end_date = get_day_start_and_end_datetime(yesterday)

    # Query for bespoke_index
    trans_s = TransactionsDocument.search()

    if profile.institution:
        inst = profile.institution
    elif inst_id:
        inst = Institution.objects.get(id=inst_id)
    else:
        inst = None

    if inst is not None:
        cache_key = (
            f"{str(inst.name).replace(' ', '') }_transaction_by_channel_{duration}"
        )
    else:
        cache_key = f"all_transaction_by_channel_{duration}"

    result = cache.get(cache_key)

    if result is not None:
        return result

    # Apply additional filters if inst_id is provided
    bes_q = Q()
    tla_q = Q()
    if inst_id:
        bes_q &= Q("term", id=inst_id)
        tla_q &= Q("term", id=inst_id)

    # Execute searches
    # bespoke_response = bespoke_search.execute()
    # tla_response = tla_search.execute()

    result = {}

    bes_q &= Q(
        "range",
        transaction_time={
            "gte": start_date.strftime("%Y-%m-%d"),
            "lte": end_date.strftime("%Y-%m-%d"),
        },
    )
    bes_q &= Q("term", department="bespoke")
    tla_q &= Q(
        "range",
        transaction_time={
            "gte": start_date.strftime("%Y-%m-%d"),
            "lte": end_date.strftime("%Y-%m-%d"),
        },
    )
    tla_q &= Q("term", department="processing")
    # Count transactions for bespoke_index
    bespoke_ussd = (
        trans_s.query(bes_q & Q("term", channel="ussd"))
        .params(scroll="5m", size=10000)
        .execute()
        .hits.total.value
    )
    bespoke_ussd_bank = (
        trans_s.query(bes_q & Q("term", channel="bankussd"))
        .params(scroll="5m", size=10000)
        .execute()
        .hits.total.value
    )
    bespoke_pos = (
        trans_s.query(bes_q & Q("term", channel="pos"))
        .params(scroll="5m", size=10000)
        .execute()
        .hits.total.value
    )
    bespoke_web = (
        trans_s.query(bes_q & Q("term", channel="web"))
        .params(scroll="5m", size=10000)
        .execute()
        .hits.total.value
    )
    bespoke_atm = (
        trans_s.query(bes_q & Q("term", channel="atm"))
        .params(scroll="5m", size=10000)
        .execute()
        .hits.total.value
    )
    bespoke_agency = (
        trans_s.query(bes_q & Q("term", channel="agency"))
        .params(scroll="5m", size=10000)
        .execute()
        .hits.total.value
    )
    bespoke_mobile = (
        trans_s.query(bes_q & Q("term", channel="mobile"))
        .params(scroll="5m", size=10000)
        .execute()
        .hits.total.value
    )

    # Count transactions for processing_index
    tla_pos = (
        trans_s.query(tla_q & Q("term", channel="2"))
        .params(scroll="5m", size=10000)
        .execute()
        .hits.total.value
    )
    tla_web = (
        trans_s.query(tla_q & Q("term", channel="2"))
        .params(scroll="5m", size=10000)
        .execute()
        .hits.total.value
    )
    tla_atm = (
        trans_s.query(tla_q & Q("term", channel="1"))
        .params(scroll="5m", size=10000)
        .execute()
        .hits.total.value
    )

    # Aggregate counts
    pos_transactions = bespoke_pos + tla_pos
    web_transactions = bespoke_web + tla_web
    atm_transactions = bespoke_atm + tla_atm

    # Prepare result dictionary
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

    return result


def monthly_transaction_report(user, inst_id):
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
    client = connections.get_connection()
    if result is None:
        result = dict()
        trans_s = TransactionsDocument.search()
        for delta in range(11, -1, -1):
            month_date = current_date - relativedelta(months=delta)
            month_start, month_end = get_month_start_and_end_datetime(month_date)

            query = Q()

            if inst:
                query &= Q("term", issuer_institution_name=inst.bespokeCode) | Q(
                    issuer_institution_name=inst.tlaCode
                )

            besp_queryset = (
                trans_s.query(
                    query
                    & Q(
                        "range",
                        transaction_time={
                            "gte": month_start.strftime("%Y-%m-%d"),
                            "lte": month_end.strftime("%Y-%m-%d"),
                        },
                    )
                    & Q("term", department="bespoke")
                )
                .params(scroll="5m", size=10000)
                .execute()
            )
            tla_queryset = (
                trans_s.query(
                    query
                    & Q(
                        "range",
                        transaction_time={
                            "gte": month_start.strftime("%Y-%m-%d"),
                            "lte": month_end.strftime("%Y-%m-%d"),
                        },
                    )
                    & Q("term", department="processing")
                )
                .params(scroll="5m", size=10000)
                .execute()
            )

            bespoke_total, _ = process_data(besp_queryset, client, "total_amount")
            tla_total, _ = process_data(tla_queryset, client, "amount")

            month_total_data = dict()
            month_total_data["month"] = (
                f"{month_start.strftime('%b')}-{str(month_start.year)[2:]}"
            )
            month_total_data["totalAmount"] = bespoke_total + tla_total

            monthly.append(month_total_data)

        result["monthly"] = monthly
        cache.set(key=cache_key, value=result, timeout=cache_timeout)
        # connections['etl_db'].close()

    return result


def card_processed_count(user, inst_id, duration):
    today = timezone.now()
    user_profile = UserDetail.objects.get(user=user)

    if duration == "" or None:
        duration = "yesterday"

    if duration == "thisMonth":
        start_date, end_date = get_month_start_and_end_datetime(today)
        prev_start_date, prev_end_date = get_month_start_and_end_datetime(start_date)
    elif duration == "thisYear":
        start_date, end_date = get_year_start_and_end_datetime(today)
        prev_start_date, prev_end_date = get_year_start_and_end_datetime(start_date)
    else:
        start_date, end_date = get_week_start_and_end_datetime(today)
        prev_start_date, prev_end_date = get_week_start_and_end_datetime(start_date)

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
        issue_tcard = CardAccountDetailsIssuingDocument.search()
        query = Q(
            "range",
            tcard_createdate={
                "gte": start_date.strftime("%Y-%m-%d"),
                "lte": end_date.strftime("%Y-%m-%d"),
            },
        )
        prev_query = Q(
            "range",
            tcard_createdate={
                "gte": prev_start_date.strftime("%Y-%m-%d"),
                "lte": prev_end_date.strftime("%Y-%m-%d"),
            },
        )
        if inst:
            query &= Q(branch__iexact=inst.issuingCode)
            prev_query &= Q(branch__iexact=inst.issuingCode)

        total_count = (
            issue_tcard.query(query)
            .params(scroll="5m", size=10000)
            .execute()
            .hits.total.value
        )
        inactive_list = [5, 6, 7, 8]
        total_count
        total_valid = (
            issue_tcard.query(query & Q("term", tcard_sign_stat=4))
            .params(scroll="5m", size=10000)
            .execute()
            .hits.total.value
        )
        prev_total_valid = (
            issue_tcard.query(prev_query & Q("term", tcard_sign_stat=4))
            .params(scroll="5m", size=10000)
            .execute()
            .hits.total.value
        )
        total_expired = (
            issue_tcard.query(query & Q("term", tcard_sign_stat=10))
            .params(scroll="5m", size=10000)
            .execute()
            .hits.total.value
        )
        prev_total_expired = (
            issue_tcard.query(prev_query & Q("term", tcard_sign_stat=10))
            .params(scroll="5m", size=10000)
            .execute()
            .hits.total.value
        )
        total_invalid = (
            issue_tcard.query(query & Q("terms", tcard_sign_stat=inactive_list))
            .params(scroll="5m", size=10000)
            .execute()
            .hits.total.value
        )
        prev_total_invalid = (
            issue_tcard.query(prev_query & Q("terms", tcard_sign_stat=inactive_list))
            .params(scroll="5m", size=10000)
            .execute()
            .hits.total.value
        )
        total_blocked = (
            issue_tcard.query(query & Q("term", tcard_sign_stat=18))
            .params(scroll="5m", size=10000)
            .execute()
            .hits.total.value
        )
        prev_total_blocked = (
            issue_tcard.query(prev_query & Q("term", tcard_sign_stat=18))
            .params(scroll="5m", size=10000)
            .execute()
            .hits.total.value
        )

        valid = expired = invalid = blocked = "-"
        if total_valid > prev_total_valid:
            valid = "+"
        if total_expired > prev_total_expired:
            expired = "+"
        if total_invalid > prev_total_invalid:
            invalid = "+"
        if total_blocked > prev_total_blocked:
            blocked = "+"

        result = {
            "totalCardProcessed": total_count,
            "totalValidCards": {
                "count": total_valid,
                "percentage": calculate_percentage_change(
                    prev_total_valid, total_valid
                ),
                "direction": valid,
            },
            "totalExpiredCards": {
                "count": total_expired,
                "percentage": calculate_percentage_change(
                    prev_total_expired, total_expired
                ),
                "direction": expired,
            },
            "totalInvalidCards": {
                "count": total_invalid,
                "percentage": calculate_percentage_change(
                    prev_total_invalid, total_invalid
                ),
                "direction": invalid,
            },
            "totalBlockedCards": {
                "count": total_blocked,
                "percentage": calculate_percentage_change(
                    prev_total_blocked, total_blocked
                ),
                "direction": blocked,
            },
        }

        cache.set(key=cache_key, value=result, timeout=cache_timeout)
        # connections['etl_db'].close()

    return result


def get_all_docno_from_elasticsearch():
    client = connections.get_connection()
    index = "settlement_detail_index"

    # Define the initial search query
    response = client.search(
        index=index,
        scroll="2m",
        size=10000,
        body={"query": {"match_all": {}}},
        _source=["DOCNO"],  # Specify the field to retrieve
    )

    # Extract docno from each hit and store them in a list
    docnos = [hit["_source"]["DOCNO"] for hit in response["hits"]["hits"]]

    return docnos


def settlement_report():
    today = timezone.now()
    current_date = timezone.now()
    cache_key = f"all_settlement_record_return_report"

    results = cache.get(cache_key)
    if results:
        return results

    result = {"daily": [], "weekly": [], "monthly": [], "yearly": []}
    # settlement_details = SettlementDetail.objects.using("etl_db").all()
    # settlement_meb = SettlementMeb.objects.using("etl_db").all()

    # all_doc_no = SettlementDetail.objects.using("etl_db").values_list(
    #     "docno", flat=True
    # )
    all_doc_no = get_all_docno_from_elasticsearch()
    settlement_meb = SettlementMebDocument.search()
    total_settle_count_yearly = 0  # Initialize total settle count for years
    total_un_settle_count_yearly = 0  # Initialize total un-settle count for years
    total_settle_count_weekly = 0  # Initialize total settle count for weeks
    total_un_settle_count_weekly = 0  # Initialize total un-settle count for weeks
    total_settle_count_monthly = 0  # Initialize total settle count for months
    total_un_settle_count_monthly = 0  # Initialize total un-settle count for months
    total_settle_count_daily = 0  # Initialize total settle count for days
    total_un_settle_count_daily = 0  # Initialize total un-settle count for days

    for delta in range(6, -1, -1):
        yesterday_date = current_date - relativedelta(days=delta)
        week_date = current_date - relativedelta(weeks=delta)
        month_date = current_date - relativedelta(months=delta)
        year_date = current_date - relativedelta(years=delta)

        day_start, day_end = get_day_start_and_end_datetime(yesterday_date)
        week_start, week_end = get_week_start_and_end_datetime(week_date)
        month_start, month_end = get_month_start_and_end_datetime(month_date)
        year_start, year_end = get_year_start_and_end_datetime(year_date)
        # daily = SettlementMeb.objects.using("etl_db").filter(docno__in=all_doc_no, createdate__range=[day_start, day_end])
        # totalqueryset = SettlementMeb.objects.using("etl_db").filter(createdate__range=[day_start, day_end]).count()
        for (
            period,
            start_date,
            end_date,
            total_settle_count_key,
            total_un_settle_count_key,
        ) in [
            (
                "weekly",
                week_start,
                week_end,
                "total_settle_count_weekly",
                "total_un_settle_count_weekly",
            ),
            (
                "monthly",
                month_start,
                month_end,
                "total_settle_count_monthly",
                "total_un_settle_count_monthly",
            ),
            (
                "yearly",
                year_start,
                year_end,
                "total_settle_count_yearly",
                "total_un_settle_count_yearly",
            ),
            (
                "daily",
                day_start,
                day_end,
                "total_settle_count_daily",
                "total_un_settle_count_daily",
            ),
        ]:
            query = Q(
                "range",
                createdate={
                    "gte": start_date.strftime("%Y-%m-%d"),
                    "lte": end_date.strftime("%Y-%m-%d"),
                },
            )

            total_count = (
                settlement_meb.query(query & Q("terms", docno=all_doc_no))
                .params(scroll="5m", size=10000)
                .execute()
                .hits.total.value
            )
            totalqueryset = (
                settlement_meb.query(query)
                .params(scroll="5m", size=10000)
                .execute()
                .hits.total.value
            )
            # total_count = queryset.count()

            if period == "yearly":
                total_settle_count_yearly += total_count
                total_un_settle_count_yearly += totalqueryset - total_count
            elif period == "weekly":
                total_settle_count_weekly += total_count
                total_un_settle_count_weekly += totalqueryset - total_count
            elif period == "monthly":
                total_settle_count_monthly += total_count
                total_un_settle_count_monthly += totalqueryset - total_count
            elif period == "daily":
                total_settle_count_daily += total_count
                total_un_settle_count_daily += totalqueryset - total_count

    # Add total settle counts to the result
    result["yearly"].append(
        {
            "year": "Total",
            "totalSettleCount": total_settle_count_yearly,
            # Adjust this based on your specific requirements
            "totalUnSettleCount": total_un_settle_count_yearly,
        }
    )

    result["weekly"].append(
        {
            "week": "Total",
            "totalSettleCount": total_settle_count_weekly,
            # Adjust this based on your specific requirements
            "totalUnSettleCount": total_un_settle_count_weekly,
        }
    )

    result["monthly"].append(
        {
            "month": "Total",
            "totalSettleCount": total_settle_count_monthly,
            # Adjust this based on your specific requirements
            "totalUnSettleCount": total_un_settle_count_monthly,
        }
    )

    result["daily"].append(
        {
            "day": "Total",
            "totalSettleCount": total_settle_count_daily,
            # Adjust this based on your specific requirements
            "totalUnSettleCount": total_un_settle_count_daily,
        }
    )

    cache.set(key=cache_key, value=result, timeout=cache_timeout)
    return result
