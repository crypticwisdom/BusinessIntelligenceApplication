from ..models import (
    CardAccountDetailsIssuing,
    Holdertags,
    CardClientPersonnelIssuing,
    Transactions,
)
from account.models import Institution
from datacore.modules.utils import log_request
from django.db.models import Q
from ..documents import TransactionsDocument
from datetime import datetime, timedelta
import ast

from elasticsearch_dsl.query import Term
from elasticsearch_dsl import Q, Search
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.query import Term, Range


def cards(data: dict):
    report_type = data.get("type", None)
    # if report_type == "number-of-card":
    #     return "card", number_of_card(data)
    # elif report_type == "activate-card":
    #     return "card", activate_card(data)
    # elif report_type == "expire-card":
    #     return "card", expire_card(data)
    # elif report_type == "valid-card":
    #     return "card", valid_card(data)
    # elif report_type == "block_card":
    #     return "card", block_card(data)
    # elif report_type == "payattitude":
    #     return "holdertag", number_of_payattitude(data)
    if report_type == "contact":
        return "transaction", contact_and_contactless(data, True)
    elif report_type == "contactless":
        return "transaction", contact_and_contactless(data, False)
    # else:
    #     return "holdertag", disable_subscription(data)


def contact_and_contactless(data: dict, difftype):

    institu: str = data.get("institution", None)
    created_start: str = data.get("createdStart", None)
    created_end: str = data.get("createdEnd", None)
    schema = ast.literal_eval(data.get("shemaNAME", "[]"))
    bins: list = ast.literal_eval(data.get("bins", "[]"))
    currency: list[str] = ast.literal_eval(data.get("currency", "[]"))
    pan: str = data.get("pan", None)
    page: str = data.get("page", 1)
    transaction_s = TransactionsDocument.search()
    q = Q()
    page = int(page) * 50
    start_page = page - 50
    log_request(f"==========>>>>> acquire started")
    print("dddddddd")
    start_date = datetime.strptime(created_start, "%Y-%m-%d") if created_start else None
    end_date = datetime.strptime(created_end, "%Y-%m-%d") if created_end else None
    if institu:
        code = Institution.objects.filter(name=institu)
        q &= Q("term", issuer_institution_name__iexact=code[0].tlaCode)
    if schema:
        schema_filters = []
        for value in schema:
            schema_filters.append(Q("prefix", pan=value))
        q &= Q("bool", should=schema_filters, minimum_should_match=1)
    if bins:
        bin_filter = []
        for value in bins:
            bin_filter.append(Q("prefix", pan=value))
        q &= Q("bool", should=bin_filter, minimum_should_match=1)

    if pan:
        q &= Q("term", pan=pan)
    if currency:
        q &= Q(
            "terms",
            account_curreny=[
                str(account_currency_value) for account_currency_value in currency
            ],
        )

    if difftype:
        q &= Q("term", pos_entry_mode=91)
    else:
        q &= Q("term", pos_entry_mode=7)

    if created_start:
        allTransaction: dict = transaction_s.query(
            q
            & Q("term", department="processing")
            & Q("range", transaction_time={"gte": created_start, "lte": created_end})
        )[start_page:page].execute()
    else:
        allTransaction: dict = transaction_s.query(
            q & Q("term", department="processing")
        )[start_page:page].execute()
    return allTransaction
