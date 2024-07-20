from dateutil.relativedelta import relativedelta
from django.core.cache import cache
from django.db.models import Count, Sum
from elasticsearch import helpers
from django.db import connections
from django.utils import timezone
from datetime import datetime, timedelta
from django.conf import settings
from elasticsearch_dsl.query import Term
from elasticsearch_dsl import Q, Search
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.query import Term, Range
from account.models import Institution, Channel, ExtraParameters, TransactionType
from ..models import UpConTerminalConfig, UpDms
from datacore.modules.utils import log_request
from ..documents import TransactionsDocument
from django.core.cache import cache
import ast

cache_timeout = settings.CACHE_TIMEOUT


def issuer_report(data):
    institu = Institution.objects.filter(name=data.get("issuerInstitutionName", ""))
    acquire_institu = Institution.objects.filter(
        name=data.get("acquireInstitution", "")
    )
    try:
        if acquire_institu.exists() or institu.exists():
            try:
                result = acquire_institu[0].acquireTlaCode
            except:
                result = False
            status, bespoke, becount = issuer_filter(
                data, institu[0].bespokeCode, False, "bespoke"
            )
            status, processing, precount = issuer_filter(
                data, institu[0].tlaCode, result, "processing"
            )
        else:
            # log_request(f"==========>>>>> all institution processing")
            status, bespoke, becount = issuer_filter(data, False, False, "bespoke")
            status, processing, precount = issuer_filter(
                data, False, False, "processing"
            )

    except Exception as e:
        return False, f"{e}", 0
    # #print("processing ,", type(processing))
    # #print("bespoke , ", type(bespoke))
    if status == False:
        return False, "Please check if the params sent are in an array", 0
    # merge_data = bespoke.union(processing)

    # merge_data.order_by("-transaction_time")
    bespoke_dicts = [hit.to_dict() for hit in bespoke]
    tla_dicts = [hit.to_dict() for hit in processing]
    combined_hits = bespoke_dicts + tla_dicts
    total = precount + becount

    # Sort combined hits by transaction date
    combined_hits_sorted = sorted(combined_hits, key=lambda x: x["transaction_time"])
    volume: int = data.get("volume", None)

    if volume:
        combined_hits_sorted: list = combined_hits_sorted[: int(volume)]
        total = volume
    return True, combined_hits_sorted, int(total)


def acquire_report(data):
    institu = Institution.objects.filter(name=data.get("acquireInstitutionName", ""))
    issuer_institu = Institution.objects.filter(name=data.get("issuerName", ""))

    # #print(institu[0].tlaCode)
    try:
        if institu.exists() or issuer_institu.exists():
            try:
                result = institu[0].tlaCode
            except:
                result = False
            # print("woring")
            status, bespoke, becount = acquire_filter(
                data, institu[0].bespokeCode, False, "bespoke"
            )
            status, processing, precount = acquire_filter(
                data, institu[0].acquireTlaCode, result, "processing"
            )

        else:
            status, bespoke, becount = acquire_filter(data, False, False, "bespoke")
            status, processing, precount = acquire_filter(
                data, False, False, "processing"
            )

    except Exception as e:
        return False, f"{e}"
    # #print("processing ,", type(processing))
    # #print("bespoke , ", type(bespoke))
    if not status:
        return False, "Please check if the params sent are in an array"
    # merge_data = bespoke.union(processing)

    # merge_data.order_by("-transaction_time")
    bespoke_dicts = [hit.to_dict() for hit in bespoke]
    tla_dicts = [hit.to_dict() for hit in processing]
    combined_hits = bespoke_dicts + tla_dicts
    total = becount + precount
    # Sort combined hits by transaction date
    combined_hits_sorted = sorted(combined_hits, key=lambda x: x["transaction_time"])
    volume: int = data.get("volume", None)
    if volume:
        volume: int = data.get("volume")
        combined_hits_sorted: list = combined_hits_sorted[: int(volume)]
        total = volume
    return True, combined_hits_sorted, int(total)


def COacquire_report(data):
    institu = Institution.objects.filter(name=data.get("acquireInstitutionName", ""))
    issuer_institu = Institution.objects.filter(name=data.get("issuerName", ""))

    # #print(institu[0].tlaCode)

    try:
        if institu.exists() or issuer_institu.exists():
            try:
                result = institu[0].tlaCode
            except:
                result = False
            status, bespoke, becount = co_acquire_filter(
                data, institu[0].bespokeCode, False, "bespoke"
            )
            status, processing, precount = co_acquire_filter(
                data, institu[0].acquireTlaCode, result, "processing"
            )

        else:
            status, bespoke, becount = co_acquire_filter(data, False, False, "bespoke")
            status, processing, precount = co_acquire_filter(
                data, False, False, "processing"
            )
    except Exception as e:
        return (
            False,
            "Please make sure the institution are provided for both issuer and acquirer",
        )
    # #print("processing ,", type(processing))
    # #print("bespoke , ", type(bespoke))
    if not status:
        return False, "Please check if the params sent are in an array"
    bespoke_dicts = [hit.to_dict() for hit in bespoke]
    tla_dicts = [hit.to_dict() for hit in processing]
    combined_hits = bespoke_dicts + tla_dicts
    total = becount + precount

    # Sort combined hits by transaction date
    combined_hits_sorted = sorted(combined_hits, key=lambda x: x["transaction_time"])
    volume: int = data.get("volume", None)
    if volume:
        volume: int = data.get("volume")
        combined_hits_sorted: list = combined_hits_sorted[: int(volume)]
        total = volume
    return True, combined_hits_sorted, int(total)


def issuer_filter(data, name, acquire_id, type) -> dict:

    try:
        issuer_institution_name: str = name

        issuer_country: list[str] = ast.literal_eval(data.get("countryCode", "[]"))
        account_currency: list[str] = ast.literal_eval(
            data.get("accountCurrency", "[]")
        )
        schema = ast.literal_eval(data.get("shemaNAME", "[]"))
        currency: list[str] = ast.literal_eval(data.get("transactionCurrency", "[]"))
        curreny_orig: list[str] = ast.literal_eval(data.get("originCurrency", "[]"))
        pan: str = data.get("pan", None)
        bin: list[str] = ast.literal_eval(data.get("issuerBin", "[]"))
        marchantId: str = data.get("marchantId", None)
        terminalId: str = data.get("terminalId", None)
        acquireBin: list[str] = ast.literal_eval(data.get("acquireBin", "[]"))
        transaction: list[str] = ast.literal_eval(data.get("acquireTransaction", "[]"))
        acquireCountry: list[str] = ast.literal_eval(
            data.get("acquireCountryCode", "[]")
        )
        channel: str = ast.literal_eval(data.get("channel", "[]"))
        transactionType: str = ast.literal_eval(data.get("transactionType", "[]"))
        domestic: str = data.get("domestic", None)
        transactionStatus: str = data.get("transactionStatus", None)
        processing: str = data.get("processing", None)
        charge: str = data.get("charge", None)
        settlement: str = data.get("settlement", None)
        start_date: str = data.get("start", None)
        end_date: str = data.get("end", None)
        volume: int = data.get("volume", None)
        AmountValue: int = data.get("value", None)
        page: int = data.get("page", None)

    except Exception as e:
        log_request(
            f"unable to filter base on incorrect type in params  ===>> error {e}"
        )
        return False, "invalid params sent"
    cache_key = f"issuer_{issuer_institution_name}_{issuer_country}_{account_currency}_{schema}_{currency}_{curreny_orig}_{pan}_{bin}_{marchantId}_{terminalId}_{acquireBin}_{transaction}_{acquireCountry}_{channel}_{transactionType}_{domestic}_{transactionStatus}_{processing}_{charge}_{settlement}_{start_date}_{end_date}_{AmountValue}_{volume}_transaction"

    if False:
        # result = cache.get(cache_key)
        # return True,result
        pass
    else:
        upbin = []
        if not processing == "others":
            upbin = (
                ExtraParameters.objects.filter(name="UPBIN").first()
                if ExtraParameters.objects.filter(name="UPBIN").first()
                else []
            )

        # combining data that are talking to the same column
        totalbin: set = set(bin).union(set(acquireBin))
        totalupbin: set = set(totalbin).union(set(upbin))
        listBin: list = list(totalupbin)
        totalbin = [str(item) for item in listBin]
        totalCurrency: set = set(currency).union(set(transaction))
        totalCurrency: list = list(totalCurrency)
        # totalCode :set= set(acquireCountry).union(set(acquireCountryDomestic))
        # totalCode :list= list(totalCode)

        # get all transactions to be filtered
        log_request(f"==========>>>>> issuer started")

        if page:
            page = int(page) * 50
            start_page = page - 50
        else:
            page = 10000
            start_page = 0
        start_date = datetime.strptime(start_date, "%Y-%m-%d") if start_date else None
        end_date = (
            datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
            if end_date
            else None
        )
        transaction_s = TransactionsDocument.search()
        q = Q()
        if issuer_institution_name:
            q &= Q("term", issuer_institution_name=issuer_institution_name)

        if schema:
            schema_filters = []
            for value in schema:
                # Assuming value is the prefix you want to filter by and constructing a regex pattern
                regex_pattern = f"{value}.*"  # This regex pattern will match PANs starting with the value
                schema_filters.append(Q("regexp", pan={"value": regex_pattern}))
            q &= Q("bool", should=schema_filters, minimum_should_match=1)

        if issuer_country:
            q &= Q(
                "terms",
                issuer_country=[
                    str(issuer_country_value) for issuer_country_value in issuer_country
                ],
            )
        if account_currency:
            q &= Q(
                "terms",
                account_curreny=[
                    str(account_currency_value)
                    for account_currency_value in account_currency
                ],
            )
        if currency:
            q &= Q(
                "terms",
                currency=[str(currency_value) for currency_value in totalCurrency],
            )
        if curreny_orig:
            q &= Q(
                "terms",
                curreny_orig=[
                    str(curreny_orig_value) for curreny_orig_value in curreny_orig
                ],
            )
        if channel:
            channel_value = []
            if type == "bespoke":
                q &= Q("terms", channel=channel)
            else:

                if (["Web", "POS"] == channel) and (["POS", "Web"] == channel):
                    # #print("value")
                    for i in channel:
                        data_tla = Channel.objects.filter(name=i)[0].tlaCode
                        # #print("channel",Channel.objects.filter(name=i)[0].tlaCode)
                        channel_value.append(data_tla)
                    q &= Q("terms", channel=channel_value)
                elif ["Web"] == channel:
                    # #print("web")
                    value = Channel.objects.filter(name="Web")[0].tlaCode
                    q &= Q(
                        "terms",
                        channel=value,
                        secondary_channel=[
                            8,
                            81,
                            82,
                            83,
                            84,
                            85,
                            86,
                            87,
                            88,
                            89,
                            61,
                            62,
                            52,
                            59,
                        ],
                    )
                else:
                    for i in channel:
                        data_tla = Channel.objects.filter(name=i)[0].tlaCode
                        channel_value.append(data_tla)

                    q &= Q("terms", channel=channel_value)
            # #print("dkkkkdkd",channel_value)
            # #print(allTransaction)
        if acquire_id:
            combinelist = ["1", acquire_id]
            q &= Q("terms", acquirer_institution_id=combinelist)
        if marchantId:
            q &= Q("term", merchant_id=marchantId)
        if terminalId:
            q &= Q("term", aquirer_terminal_id=terminalId)

        if transactionType:
            trans = []
            final_trans = []
            if type == "bespoke":
                for i in transactionType:
                    trans.append(TransactionType.objects.filter(name=i)[0].bespokeCode)
                for tran in trans:
                    remove_split = tran.split(",")
                    final_trans += remove_split
            else:
                for i in transactionType:
                    trans.append(TransactionType.objects.filter(name=i)[0].tlaCode)
                for tran in trans:
                    remove_split = tran.split(",")
                    final_trans += remove_split
            q &= Q("terms", transaction_type=final_trans)
        if transactionStatus:
            if transactionStatus == "approved":
                if type == "bespoke":
                    q &= Q("term", transaction_status="true")
                else:
                    q &= Q("term", transaction_status="1")
            elif transactionStatus == "declined":
                if type == "bespoke":
                    q &= ~Q("term", transaction_status="false")
                else:
                    q &= ~Q("term", transaction_status="1")
        if pan:
            q &= Q("term", pan=pan)
        if totalbin:
            bin_filter = []
            for value in totalbin:
                bin_filter.append(Q("prefix", pan=value))
            q &= Q("bool", should=bin_filter, minimum_should_match=1)
        if acquireCountry:
            q &= Q("terms", acquirer_country=acquireCountry)

        # if start_date and end_date:

        #     end_date = datetime.strptime(end_date, "%Y-%m-%d")

        #     # Increase the end_date by one day
        #     end_date = end_date + timedelta(days=1)

        #     q &= Q(transaction_time__range=(start_date,end_date))
        # allTransaction.order_by("-transaction_time")
        if domestic:

            if domestic == "domestic":
                q &= Q("term", acquirer_country=566)
            else:
                q &= ~Q("term", acquirer_country=566)
        if AmountValue:
            print("dddd", AmountValue)
            operator_mapping = {
                ">=": "gte",
                ">": "gt",
                "<=": "lte",
                "<": "lt",
                "=": "eq",
            }

            # Initialize operator and amount_value
            operator = ""
            amount_value = AmountValue

            # Iterate over operator_mapping to find the matching operator
            for op, lookup in operator_mapping.items():
                if AmountValue.endswith(op):
                    operator = lookup
                    amount_value = AmountValue[
                        : -len(op)
                    ].rstrip()  # Remove the operator and any trailing whitespace
                    break
            if type == "bespoke":
                if operator:
                    print(amount_value, AmountValue)
                    if operator == "eq":
                        q &= Q("term", **{"total_amount": amount_value})
                    else:
                        q &= Q("range", **{"total_amount": {operator: amount_value}})
            else:
                if operator:
                    print(amount_value, AmountValue)
                    if operator == "eq":
                        q &= Q("term", **{"amount": amount_value})
                    else:
                        q &= Q("range", **{"amount": {operator: amount_value}})

        if charge:
            if charge.lower() == "yes":
                # values_to_check = allTransaction.values_list('rrn', flat=True)

                queryset_updms = (
                    UpDms.objects.using("etl_db")
                    .filter()
                    .values_list("trans_id", flat=True)
                )

                q &= Q("terms", rrn=queryset_updms)

        # if settlement:
        #     if settlement.lower() == "yes":
        #         values_to_check = allTransaction.values_list('rrn', flat=True)

        #         queryset_updms = SettlementDetail.objects.using("etl_db").filter(trannumber__in=values_to_check)

        #         q &= Q('terms',rrn=queryset_updms.values_list('trannumber', flat=True))

        allTransaction: dict = transaction_s.query(
            q
            & Q("term", department=type)
            & Q("range", transaction_time={"gte": start_date, "lt": end_date})
        )[start_page:page].execute()
        count = (
            transaction_s.query(
                q
                & Q("term", department=type)
                & Q("range", transaction_time={"gte": start_date, "lt": end_date})
            )
            .execute()
            .hits.total.value
        )

        # print(allTransaction, "hhdhd")
    # cache.set(key=cache_key, value=allTransaction, timeout=cache_timeout)
    print(transaction)
    return True, allTransaction, count


def acquire_filter(data, name, issuer_name, type):
    try:
        acquire_institution_name: str = name
        currency: list[str] = ast.literal_eval(data.get("transactionCurrency", "[]"))
        curreny_orig: list[str] = ast.literal_eval(data.get("originCurrency", "[]"))
        pan: str = data.get("pan", None)
        bin: list[str] = ast.literal_eval(data.get("issuerBin", "[]"))
        issuer_country: list[str] = ast.literal_eval(data.get("countryCode", "[]"))
        marchantId: str = data.get("marchantId", None)
        # issuerName :str = data.get('issuerName', None)
        schema: str = ast.literal_eval(data.get("schema", "[]"))
        terminalId: str = data.get("terminalId", None)
        acquireBin: list[str] = ast.literal_eval(data.get("acquireBin", "[]"))
        transaction: list[str] = ast.literal_eval(data.get("acquireTransaction", "[]"))
        acquireCountry: list[str] = ast.literal_eval(
            data.get("acquireCountryCode", "[]")
        )
        domestic: str = data.get("domestic", None)
        transactionStatus: str = data.get("transactionStatus", None)
        transactionType: str = ast.literal_eval(data.get("transactionType", "[]"))
        processing: str = data.get("processing", None)
        charge: str = data.get("charge", None)
        settlement: str = data.get("settlement", None)
        AmountValue: int = data.get("value", None)
        start_date: str = data.get("start", None)
        end_date: str = data.get("end", None)
        volume: int = data.get("volume", None)
        page: int = data.get("page", None)
    except Exception as e:
        log_request(
            f"unable to filter base on incorrect type in params  ===>> error {e}"
        )
        return False, f"invalid params sent "
    upbin = []
    if not processing == "others":
        upbin = (
            ExtraParameters.objects.filter(name="UPBIN").first()
            if ExtraParameters.objects.filter(name="UPBIN").first()
            else []
        )
    # combining data that are talking to the same column
    totalbin: set = set(bin).union(set(acquireBin))
    totalupbin: set = set(totalbin).union(set(upbin))
    listBin: list = list(totalupbin)
    listBin: list = list(totalbin)
    totalbin = [str(item) for item in listBin]
    totalCurrency: set = set(currency).union(set(transaction))
    totalCurrency: list = list(totalCurrency)
    if page:
        page = int(page) * 50
        start_page = page - 50
    else:
        page = 10000
        start_page = 0
    # totalCode :set= set(acquireCountry).union(set(acquireCountryDomestic))
    # totalCode :list= list(totalCode)

    # get all transactions to be filtered
    log_request(f"==========>>>>> acquire started")
    start_date = datetime.strptime(start_date, "%Y-%m-%d") if start_date else None
    end_date = (
        datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        if end_date
        else None
    )

    # Increase the end_date by one day
    # end_date = end_date + timedelta(days=1)
    # #print("acquire_institution_name",acquire_institution_name,issuer_name)
    transaction_s = TransactionsDocument.search()
    q = Q()
    if acquire_institution_name:
        combineList = ["1", acquire_institution_name]
        q &= Q("terms", acquirer_institution_id=combineList)

    if totalbin:
        bin_filter = []
        for value in totalbin:
            bin_filter.append(Q("prefix", pan=value))
        q &= Q("bool", should=bin_filter, minimum_should_match=1)
    if currency:
        q &= Q(
            "terms", currency=[str(currency_value) for currency_value in totalCurrency]
        )
    if terminalId:
        q &= Q("term", aquirer_terminal_id=terminalId)
    if marchantId:
        q &= Q("term", merchant_id=marchantId)
    if curreny_orig:
        q &= Q(
            "terms",
            curreny_orig=[
                str(curreny_orig_value) for curreny_orig_value in curreny_orig
            ],
        )
    if acquireCountry:
        q &= Q(
            "terms",
            acquirer_country=[
                str(acquirer_country_value) for acquirer_country_value in acquireCountry
            ],
        )
    if issuer_name:

        q &= Q("term", issuer_institution_name=issuer_name)
    if schema:
        schema_filters = []
        for value in schema:
            schema_filters.append(Q("prefix", pan=value))
        q &= Q("bool", should=schema_filters, minimum_should_match=1)

    if issuer_country:
        q &= Q(
            "terms",
            issuer_country=[
                str(issuer_country_value) for issuer_country_value in issuer_country
            ],
        )

    if pan:
        q &= Q("term", pan=pan)

    if transactionType:
        trans = []
        final_trans = []
        if type == "bespoke":
            for i in transactionType:
                trans.append(TransactionType.objects.filter(name=i)[0].bespokeCode)
            for tran in trans:
                remove_split = tran.split(",")
                final_trans += remove_split
        else:
            for i in transactionType:
                trans.append(TransactionType.objects.filter(name=i)[0].tlaCode)
            for tran in trans:
                remove_split = tran.split(",")
                final_trans += remove_split
        q &= Q("terms", transaction_type=final_trans)

    if transactionStatus:
        if transactionStatus == "approved":
            if type == "bespoke":
                q &= Q("term", transaction_status="true")
            else:
                q &= Q("term", transaction_status="1")
        elif transactionStatus == "declined":
            if type == "bespoke":
                q &= ~Q("term", transaction_status="false")
            else:
                q &= ~Q("term", transaction_status="1")

    if domestic:
        if domestic == "domestic":
            q &= Q("term", acquirer_country=566)
        else:
            q &= ~Q("term", acquirer_country=566)
    if AmountValue:

        operator_mapping = {
            ">=": "__gte",
            ">": "__gt",
            "<=": "__lte",
            "<": "__lt",
            "=": "",
        }

        # Initialize operator and amount_value
        operator = ""
        amount_value = AmountValue

        # Iterate over operator_mapping to find the matching operator
        for op, lookup in operator_mapping.items():
            if AmountValue.endswith(op):
                operator = lookup
                amount_value = AmountValue[
                    : -len(op)
                ].rstrip()  # Remove the operator and any trailing whitespace
                break
        if type == "bespoke":
            if operator:
                print(amount_value, AmountValue)
                if operator == "eq":
                    q &= Q("term", **{"total_amount": amount_value})
                else:
                    q &= Q("range", **{"total_amount": {operator: amount_value}})
        else:
            if operator:
                print(amount_value, AmountValue)
                if operator == "eq":
                    q &= Q("term", **{"amount": amount_value})
                else:
                    q &= Q("range", **{"amount": {operator: amount_value}})
    if charge:
        if charge.lower() == "yes":
            values_to_check = allTransaction.values_list("rrn", flat=True)

            queryset_updms = UpDms.objects.using("etl_db").filter(
                trans_id__in=values_to_check
            )

            q &= Q("terms", rrn=queryset_updms.values_list("trans_id", flat=True))

    # if settlement:
    #     if settlement.lower() == "yes":
    #         values_to_check = allTransaction.values_list('rrn', flat=True)

    #         queryset_updms = SettlementDetail.objects.using("etl_db").filter(trannumber__in=values_to_check)

    #         q &= Q(rrn__in=queryset_updms.values_list('trannumber', flat=True))

    allTransaction: dict = transaction_s.query(
        q
        & Q("term", department=type)
        & Q("range", transaction_time={"gte": start_date, "lt": end_date})
    )[start_page:page].execute()
    count = (
        transaction_s.query(
            q
            & Q("term", department=type)
            & Q("range", transaction_time={"gte": start_date, "lt": end_date})
        )
        .execute()
        .hits.total.value
    )
    # if volume:
    #     allTransaction = allTransaction[: int(volume)]

    return True, allTransaction, count


# def issuerInstitution_info(request):
#     issuer_institution_name :str= request.GET.get('name', None)
#     issuer_info :Transactions = Transactions.objects.using("etl_db").filter(issuer_institution_name__iexact=issuer_institution_name)
#     if issuer_info.exists():
#         result = {
#             "countryCode":[issuer.issuer_country for issuer in issuer_info],
#             "accountCurrency": [issuer.account_curreny for issuer in issuer_info],
#             "transactionCurrency":[issuer.currency for issuer in issuer_info],
#             "originCurrency":[issuer.curreny_orig for issuer in issuer_info],
#             "curreny_orig":[issuer.currency for issuer in issuer_info]
#         }
#         return False,result


def co_acquire_filter(data, name, issuer_name, type):
    try:
        acquire_institution_name: str = name
        currency: list[str] = ast.literal_eval(data.get("transactionCurrency", "[]"))
        curreny_orig: list[str] = ast.literal_eval(data.get("originCurrency", "[]"))
        pan: str = data.get("pan", None)
        bin: list[str] = ast.literal_eval(data.get("issuerBin", "[]"))
        issuer_country: list[str] = ast.literal_eval(data.get("countryCode", "[]"))
        marchantId: str = data.get("marchantId", None)
        # issuerName :str = data.get('issuerName', None)
        schema: str = ast.literal_eval(data.get("schema", "[]"))
        terminalId: str = data.get("terminalId", None)
        acquireBin: list[str] = ast.literal_eval(data.get("acquireBin", "[]"))
        transaction: list[str] = ast.literal_eval(data.get("acquireTransaction", "[]"))
        acquireCountry: list[str] = ast.literal_eval(
            data.get("acquireCountryCode", "[]")
        )
        domestic: str = data.get("domestic", None)
        transactionStatus: str = data.get("transactionStatus", None)
        transactionType: str = ast.literal_eval(data.get("transactionType", "[]"))
        processing: str = data.get("processing", None)
        charge: str = data.get("charge", None)
        settlement: str = data.get("settlement", None)
        AmountValue: int = data.get("value", None)
        start_date: str = data.get("start", None)
        end_date: str = data.get("end", None)
        volume: int = data.get("volume", None)
        page: int = data.get("page", None)

    except Exception as e:
        log_request(
            f"unable to filter base on incorrect type in params  ===>> error {e}"
        )
        return False, f"invalid params sent "
    upbin = []
    if not processing == "others":
        upbin = (
            ExtraParameters.objects.filter(name="UPBIN").first()
            if ExtraParameters.objects.filter(name="UPBIN").first()
            else []
        )
    # combining data that are talking to the same column
    totalbin: set = set(bin).union(set(acquireBin))
    totalupbin: set = set(totalbin).union(set(upbin))
    listBin: list = list(totalupbin)
    listBin: list = list(totalbin)
    totalbin = [str(item) for item in listBin]
    totalCurrency: set = set(currency).union(set(transaction))
    totalCurrency: list = list(totalCurrency)
    # totalCode :set= set(acquireCountry).union(set(acquireCountryDomestic))
    # totalCode :list= list(totalCode)

    # get all transactions to be filtered
    log_request(f"==========>>>>> acquire started")
    start_date = datetime.strptime(start_date, "%Y-%m-%d") if start_date else None
    end_date = (
        datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        if end_date
        else None
    )
    if page:
        page = int(page) * 50
        start_page = page - 50
    else:
        page = 10000
        start_page = 0
    # Increase the end_date by one day
    end_date = end_date + timedelta(days=1)
    # #print("acquire_institution_name",acquire_institution_name,issuer_name)
    transaction_s = TransactionsDocument.search()
    q = Q()
    if acquire_institution_name:
        combineList = ["1", acquire_institution_name]
        q &= Q("terms", acquirer_institution_id=combineList)

    if totalbin:
        bin_filter = []
        for value in totalbin:
            bin_filter.append(Q("prefix", pan=value))
        q &= Q("bool", should=bin_filter, minimum_should_match=1)
    if currency:
        q &= Q(
            "terms", currency=[str(currency_value) for currency_value in totalCurrency]
        )
    if terminalId:
        q &= Q("term", aquirer_terminal_id=terminalId)
    if marchantId:
        q &= Q("term", merchant_id=marchantId)
    if curreny_orig:
        q &= Q(
            "terms",
            curreny_orig=[
                str(curreny_orig_value) for curreny_orig_value in curreny_orig
            ],
        )
    if acquireCountry:
        q &= Q(
            "terms",
            acquirer_country=[
                str(acquirer_country_value) for acquirer_country_value in acquireCountry
            ],
        )
    if issuer_name:

        q &= Q("term", issuer_institution_name=issuer_name)
    if schema:
        schema_filters = []
        for value in schema:
            schema_filters.append(Q("prefix", pan=value))
        q &= Q("bool", should=schema_filters, minimum_should_match=1)

    if issuer_country:
        q &= Q(
            "terms",
            issuer_country=[
                str(issuer_country_value) for issuer_country_value in issuer_country
            ],
        )

    if pan:
        q &= Q("term", pan=pan)

    if transactionType:
        trans = []
        final_trans = []
        if type == "bespoke":
            for i in transactionType:
                trans.append(TransactionType.objects.filter(name=i)[0].bespokeCode)
            for tran in trans:
                remove_split = tran.split(",")
                final_trans += remove_split
        else:
            for i in transactionType:
                trans.append(TransactionType.objects.filter(name=i)[0].tlaCode)
            for tran in trans:
                remove_split = tran.split(",")
                final_trans += remove_split
        q &= Q("terms", transaction_type=final_trans)

    if transactionStatus:
        if transactionStatus == "approved":
            if type == "bespoke":
                q &= Q("term", transaction_status="true")
            else:
                q &= Q("term", transaction_status="1")
        elif transactionStatus == "declined":
            if type == "bespoke":
                q &= ~Q("term", transaction_status="false")
            else:
                q &= ~Q("term", transaction_status="1")

    if domestic:
        if domestic == "domestic":
            q &= Q("term", acquirer_country=566)
        else:
            q &= ~Q("term", acquirer_country=566)
    if AmountValue:

        operator_mapping = {
            ">=": "__gte",
            ">": "__gt",
            "<=": "__lte",
            "<": "__lt",
            "=": "",
        }

        # Initialize operator and amount_value
        operator = ""
        amount_value = AmountValue

        # Iterate over operator_mapping to find the matching operator
        for op, lookup in operator_mapping.items():
            if AmountValue.endswith(op):
                operator = lookup
                amount_value = AmountValue[
                    : -len(op)
                ].rstrip()  # Remove the operator and any trailing whitespace
                break
        if type == "bespoke":
            if operator:
                print(amount_value, AmountValue)
                if operator == "eq":
                    q &= Q("term", **{"total_amount": amount_value})
                else:
                    q &= Q("range", **{"total_amount": {operator: amount_value}})
        else:
            if operator:
                print(amount_value, AmountValue)
                if operator == "eq":
                    q &= Q("term", **{"amount": amount_value})
                else:
                    q &= Q("range", **{"amount": {operator: amount_value}})
    if charge:
        if charge.lower() == "yes":
            values_to_check = allTransaction.values_list("rrn", flat=True)

            queryset_updms = UpDms.objects.using("etl_db").filter(
                trans_id__in=values_to_check
            )

            q &= Q("terms", rrn=queryset_updms.values_list("trans_id", flat=True))

    # if settlement:
    #     if settlement.lower() == "yes":
    #         values_to_check = allTransaction.values_list('rrn', flat=True)

    #         queryset_updms = SettlementDetail.objects.using("etl_db").filter(trannumber__in=values_to_check)

    #         q &= Q(rrn__in=queryset_updms.values_list('trannumber', flat=True))
    allTransaction: dict = transaction_s.query(
        q
        & Q("term", department=type)
        & Q("range", transaction_time={"gte": start_date, "lt": end_date})
    )[start_page:page].execute()
    count = (
        transaction_s.query(
            q
            & Q("term", department=type)
            & Q("range", transaction_time={"gte": start_date, "lt": end_date})
        )
        .execute()
        .hits.total.value
    )
    # if volume:
    #     allTransaction = allTransaction[: int(volume)]

    return True, allTransaction, count
