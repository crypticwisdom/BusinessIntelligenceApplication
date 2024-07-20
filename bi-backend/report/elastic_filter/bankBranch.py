from dateutil.relativedelta import relativedelta
from django.core.cache import cache
from django.db.models import Count, Sum
from elasticsearch import helpers
from django.db import connections
from django.utils import timezone
from datetime import datetime,timedelta

from django.conf import settings
from elasticsearch_dsl.query import Term
from ..models import  UpDms, SettlementDetail,BankBranchEtl
from elasticsearch_dsl import Q, Search
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.query import Term, Range
from account.models import UserDetail, Institution
from datacore.modules.utils import get_previous_date, get_year_start_and_end_datetime, \
    get_month_start_and_end_datetime, get_week_start_and_end_datetime, get_day_start_and_end_datetime, \
    calculate_percentage_change
from report.models import Transactions, CardAccountDetailsIssuing,UpConTerminalConfig,SettlementMeb,SettlementDetail
cache_timeout = settings.CACHE_TIMEOUT

def bankBranch(data: dict):
    # print("data woring")
    issuer_institu: str = data.get("issuer", None)
    acquirer_institu: str = data.get("acquirer", None)
    value: str = data.get("value", None)
    domestic: str = data.get("domestic", None)
    charged_back: str = data.get("chargeBack", None)
    settlement: str = data.get("settlement", None)
    status: str = data.get("transactionStatus", None)
    start_date: datetime = data.get("startDate", None)
    end_date: datetime = data.get("endDate", None)
    volume: str = data.get("volume", None)
    client = connections.get_connection()
    # Create a Search object
    branch = Search(index="bankBranch_index")
    q = Q()
    if issuer_institu:
        code = Institution.objects.filter(name=issuer_institu)
        q &= Q('term',bank_code=code[0].bespokeCode)

    if value:

        operator_mapping = {
            '>=': '__gte',
            '>': '__gt',
            '<=': '__lte',
            '<': '__lt',
            '=': '',
        }

        # Initialize operator and amount_value
        operator = ''
        amount_value = value

        # Iterate over operator_mapping to find the matching operator
        for op, lookup in operator_mapping.items():
            if value.endswith(op):
                operator = lookup
                amount_value = value[:-len(op)].rstrip()  # Remove the operator and any trailing whitespace
                break

        if operator:
            lookup_field = f'tran_amount{operator}'
            q &= Q('term',**{lookup_field: amount_value})
            
    if charged_back:
        if charged_back.lower() == "yes":
            # values_to_check = BankBranchEtl.objects.values_list('rrn', flat=True)

            queryset_updms = UpDms.objects.using("etl_db").values_list('trans_id',flat=True)

            q &= Q("terms",rrn__in=queryset_updms)

    if settlement:
        if settlement.lower() == "yes":
            # values_to_check = BankBranchEtl.objects.values_list('rrn', flat=True)

            queryset_updms = SettlementDetail.objects.using("etl_db").values_list('trannumber__in')

            q &= Q("terms",rrn__in=queryset_updms)
    if status:
        if status == "approved":
            q &= Q(response_code="00")
        elif status == "declined":
            q &= ~Q(response_code="00")
    if start_date and end_date:
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

        # Increase the end_date by one day
        end_date = end_date + timedelta(days=1)        
        q &= Q("range",tran_data__range={'gte': start_date.strftime('%Y-%m-%d %H:%M:%S'), 'lte': end_date.strftime('%Y-%m-%d %H:%M:%S')})

    bankBranch_search = Search(using=client, index=branch)
    response_query = bankBranch_search.query(q)[0:1000]

    # if volume:

    #     bank_branch = bank_branch[:int(volume)]

    return response_query

