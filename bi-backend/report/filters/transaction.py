from ..models import Transactions, UpDms, SettlementDetail
from account.models import Institution, Channel, ExtraParameters, TransactionType
from datacore.modules.utils import log_request
from django.db.models import Q
from datetime import datetime, timedelta
from django.core.cache import cache
import ast
from django.conf import settings

cache_timeout = settings.CACHE_TIMEOUT


def issuer_report_db(data):
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
            status, bespoke = issuer_filter(
                data, institu[0].bespokeCode, False, "bespoke"
            )
            status, processing = issuer_filter(
                data, institu[0].tlaCode, result, "processing"
            )
        else:
            log_request(f"==========>>>>> all institution processing")
            status, bespoke = issuer_filter(data, False, False, "bespoke")
            status, processing = issuer_filter(data, False, False, "processing")

    except Exception as e:
        return False, f"Please make sure the institution is provided {e}"
    # print("processing ,", type(processing))
    # print("bespoke , ", type(bespoke))
    if status == False:
        return False, "Please check if the params sent are in an array"
    merge_data = bespoke.union(processing)

    merge_data.order_by("-transaction_time")
    return True, merge_data


def acquire_report_db(data):
    institu = Institution.objects.filter(name=data.get("acquireInstitutionName", ""))
    issuer_institu = Institution.objects.filter(name=data.get("issuerName", ""))

    # print(institu[0].tlaCode)
    try:
        if institu.exists() or issuer_institu.exists():
            try:
                result = institu[0].tlaCode
            except:
                result = False
            status, bespoke = acquire_filter(
                data, institu[0].bespokeCode, False, "bespoke"
            )
            status, processing = acquire_filter(
                data, issuer_institu[0].acquireTlaCode, result, "processing"
            )

        else:
            status, bespoke = acquire_filter(data, False, False, "bespoke")
            status, processing = acquire_filter(data, False, False, "processing")

    except Exception as e:
        return (
            False,
            f"Please make sure the institution are provided for both issuer and acquirer {e}",
        )
    # print("processing ,", type(processing))
    # print("bespoke , ", type(bespoke))
    if not status:
        return False, "Please check if the params sent are in an array"
    merge_data = bespoke.union(processing)

    merge_data.order_by("-transaction_time")
    return True, merge_data


def COacquire_report_db(data):
    institu = Institution.objects.filter(name=data.get("acquireInstitutionName", ""))
    issuer_institu = Institution.objects.filter(name=data.get("issuerName", ""))

    # print(institu[0].tlaCode)

    try:
        if institu.exists() or issuer_institu.exists():
            try:
                result = institu[0].tlaCode
            except:
                result = False
            status, bespoke = co_acquire_filter(
                data, institu[0].bespokeCode, False, "bespoke"
            )
            status, processing = co_acquire_filter(
                data, issuer_institu[0].acquireTlaCode, result, "processing"
            )

        else:
            status, bespoke = co_acquire_filter(data, False, False, "bespoke")
            status, processing = co_acquire_filter(data, False, False, "processing")
    except Exception as e:
        return (
            False,
            "Please make sure the institution are provided for both issuer and acquirer",
        )
    # print("processing ,", type(processing))
    # print("bespoke , ", type(bespoke))
    if not status:
        return False, "Please check if the params sent are in an array"
    merge_data = bespoke.union(processing)

    merge_data.order_by("-transaction_time")
    return True, merge_data


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

    except Exception as e:
        log_request(
            f"unable to filter base on incorrect type in params  ===>> error {e}"
        )
        return False, "invalid params sent"
    cache_key = f"issuer_{issuer_institution_name}_{issuer_country}_{account_currency}_{schema}_{currency}_{curreny_orig}_{pan}_{bin}_{marchantId}_{terminalId}_{acquireBin}_{transaction}_{acquireCountry}_{channel}_{transactionType}_{domestic}_{transactionStatus}_{processing}_{charge}_{settlement}_{start_date}_{end_date}_{AmountValue}_{volume}_transaction"
    p = "p"
    if cache.get(p):
        result = cache.get(cache_key)
        return True, result
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
        q = Q()
        if issuer_institution_name:
            q &= Q(issuer_institution_name__iexact=issuer_institution_name)
        if issuer_institution_name:
            q += f" issuer_institution_name ilike {issuer_institution_name} "
        if schema:

            schema_filters = Q()
            for value in schema:
                schema_filters |= Q(pan__istartswith=str(value))
            q &= Q(schema_filters)

        if issuer_country:
            q &= Q(
                issuer_country__in=[
                    str(issuer_country_value) for issuer_country_value in issuer_country
                ]
            )
        if account_currency:
            q &= Q(
                account_curreny__in=[
                    str(account_currency_value)
                    for account_currency_value in account_currency
                ]
            )
        if currency:
            q &= Q(
                currency__in=[str(currency_value) for currency_value in totalCurrency]
            )
        if curreny_orig:
            q &= Q(
                curreny_orig__in=[
                    str(curreny_orig_value) for curreny_orig_value in curreny_orig
                ]
            )
        if channel:
            channel_value = []
            if type == "bespoke":
                q &= Q(channel__in=channel)
            else:

                if (["Web", "POS"] == channel) and (["POS", "Web"] == channel):
                    # print("value")
                    for i in channel:
                        data_tla = Channel.objects.filter(name=i)[0].tlaCode
                        # print("channel",Channel.objects.filter(name=i)[0].tlaCode)
                        channel_value.append(data_tla)
                    q &= Q(channel__in=channel_value)
                elif ["Web"] == channel:
                    # print("web")
                    value = Channel.objects.filter(name="Web")[0].tlaCode
                    q &= Q(
                        channel__in=value,
                        secondary_channel__in=[
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

                    q &= Q(channel__in=channel_value)
            # print("dkkkkdkd",channel_value)
            # print(allTransaction)
        if acquire_id:
            combinelist = ["1", acquire_id]
            q &= Q(acquirer_institution_id__in=combinelist)
        if marchantId:
            q &= Q(merchant_id__iexact=marchantId)
        if terminalId:
            q &= Q(aquirer_terminal_id__iexact=terminalId)

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
            q &= Q(transaction_type__in=final_trans)
        if transactionStatus:
            if transactionStatus == "approved":
                if type == "bespoke":
                    q &= Q(transaction_status__iexact="true")
                else:
                    q &= Q(transaction_status__iexact="1")
            elif transactionStatus == "declined":
                if type == "bespoke":
                    q &= ~Q(transaction_status__iexact="false")
                else:
                    q &= ~Q(transaction_status__iexact="1")
        if pan:
            q &= Q(pan__iexact=pan)
        if totalbin:
            totalbin_filter = Q()
            for bins in totalbin:
                totalbin_filter |= Q(pan__startswith=bins)

            q &= Q(totalbin_filter)
        if acquireCountry:
            q &= Q(acquirer_country__in=acquireCountry)

        if start_date and end_date:

            end_date = datetime.strptime(end_date, "%Y-%m-%d")

            # Increase the end_date by one day
            end_date = end_date + timedelta(days=1)

            q &= Q(transaction_time__range=(start_date, end_date))
        if domestic:

            if domestic == "domestic":
                q &= Q(acquirer_country=566)
            else:
                q &= ~Q(acquirer_country=566)
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

            if operator:
                lookup_field = f"amount{operator}"
                q &= Q(**{lookup_field: amount_value})

        if charge:
            if charge.lower() == "yes":
                values_to_check = allTransaction.values_list("rrn", flat=True)

                queryset_updms = UpDms.objects.using("etl_db").filter(
                    trans_id__in=values_to_check
                )

                q &= Q(rrn__in=queryset_updms.values_list("trans_id", flat=True))

        if settlement:
            if settlement.lower() == "yes":
                values_to_check = allTransaction.values_list("rrn", flat=True)

                queryset_updms = SettlementDetail.objects.using("etl_db").filter(
                    trannumber__in=values_to_check
                )

                q &= Q(rrn__in=queryset_updms.values_list("trannumber", flat=True))

        allTransaction: Transactions = Transactions.objects.using("etl_db").filter(
            q, department=type
        )
        allTransaction = allTransaction.order_by("-transaction_time")

        if volume:
            allTransaction = allTransaction[: int(volume)]
    cache.set(key=cache_key, value=allTransaction, timeout=cache_timeout)
    return True, allTransaction


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
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # Increase the end_date by one day
    end_date = end_date + timedelta(days=1)
    # print("acquire_institution_name",acquire_institution_name,issuer_name)
    q = Q()
    if acquire_institution_name:
        combineList = ["1", acquire_institution_name]
        q &= Q(acquirer_institution_id__in=combineList)

    if totalbin:
        totalbin_filter = Q()
        for bins in totalbin:
            totalbin_filter |= Q(pan__startswith=bins)

        allTransaction = allTransaction.filter(totalbin_filter)
    if currency:
        q &= Q(currency__in=[str(currency_value) for currency_value in totalCurrency])
    if terminalId:
        q &= Q(aquirer_terminal_id__iexact=terminalId)
    if marchantId:
        q &= Q(merchant_id__iexact=marchantId)
    if curreny_orig:
        q &= Q(
            curreny_orig__in=[
                str(curreny_orig_value) for curreny_orig_value in curreny_orig
            ]
        )
    if acquireCountry:
        q &= Q(
            acquirer_country__in=[
                str(acquirer_country_value) for acquirer_country_value in acquireCountry
            ]
        )
    if issuer_name:

        q &= Q(issuer_institution_name__iexact=issuer_name)
    if schema:

        schema_filters = Q()
        for value in schema:
            schema_filters |= Q(pan__istartswith=str(value))
        q &= Q(schema_filters)
    if issuer_country:
        q &= Q(
            issuer_country__in=[
                str(issuer_country_value) for issuer_country_value in issuer_country
            ]
        )
    if pan:
        q &= Q(pan__iexact=pan)

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
        q &= Q(transaction_type__in=final_trans)

    if transactionStatus:
        if transactionStatus == "approved":
            if type == "bespoke":
                q &= Q(transaction_status__iexact="true")
            else:
                q &= Q(transaction_status__iexact="1")
        elif transactionStatus == "declined":
            if type == "bespoke":
                q &= ~Q(transaction_status__iexact="false")
            else:
                q &= ~Q(transaction_status__iexact="1")

    if domestic:
        if domestic == "domestic":
            q &= Q(acquirer_country=566)
        else:
            q &= ~Q(acquirer_country=566)
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

        if operator:
            lookup_field = f"amount{operator}"
            q &= Q(**{lookup_field: amount_value})
    if charge:
        if charge.lower() == "yes":
            values_to_check = allTransaction.values_list("rrn", flat=True)

            queryset_updms = UpDms.objects.using("etl_db").filter(
                trans_id__in=values_to_check
            )

            q &= Q(some_field__in=queryset_updms.values_list("trans_id", flat=True))

    if settlement:
        if settlement.lower() == "yes":
            values_to_check = allTransaction.values_list("rrn", flat=True)

            queryset_updms = SettlementDetail.objects.using("etl_db").filter(
                trannumber__in=values_to_check
            )

            q &= Q(rrn__in=queryset_updms.values_list("trannumber", flat=True))
    allTransaction: Transactions = Transactions.objects.using("etl_db").filter(
        q, department=type, transaction_time__range=(start_date, end_date)
    )

    if volume:
        allTransaction = allTransaction[: int(volume)]

    return True, allTransaction


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
        transactionType: str = data.get("transactionType", None)
        processing: str = data.get("processing", None)
        charge: str = data.get("charge", None)
        settlement: str = data.get("settlement", None)
        AmountValue: int = data.get("value", None)
        start_date: str = data.get("start", None)
        end_date: str = data.get("end", None)
        volume: int = data.get("volume", None)
    except Exception as e:
        log_request(
            f"unable to filter base on incorrect type in params  ===>> error {e}"
        )
        return False, "invalid params sent"
    upbin = []
    if not processing == "others":
        upbin = (
            ExtraParameters.objects.filter(name="UPBIN").first()
            if ExtraParameters.objects.filter(name="UPBIN").first()
            else []
        )
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # Increase the end_date by one day
    end_date = end_date + timedelta(days=1)
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
    q = Q()
    # print("acquire_institution_name",acquire_institution_name,issuer_name)
    if acquire_institution_name:
        combineList = ["1", acquire_institution_name]
        q &= Q(acquirer_institution_id__in=combineList)

    if totalbin:
        totalbin_filter = Q()
        for bins in totalbin:
            totalbin_filter |= Q(pan__startswith=bins)

        allTransaction = allTransaction.filter(totalbin_filter)

    if currency:
        q &= Q(currency__in=[str(currency_value) for currency_value in totalCurrency])
    if terminalId:
        q &= Q(aquirer_terminal_id__iexact=terminalId)
    if marchantId:
        q &= Q(merchant_id__iexact=marchantId)
    if curreny_orig:
        q &= Q(
            curreny_orig__in=[
                str(curreny_orig_value) for curreny_orig_value in curreny_orig
            ]
        )
    if acquireCountry:
        q &= Q(
            acquirer_country__in=[
                str(acquirer_country_value) for acquirer_country_value in acquireCountry
            ]
        )
    if issuer_name:

        q &= Q(issuer_institution_name__iexact=issuer_name)
    if schema:
        schema_filters = []
        for value in schema:
            # Construct a regex pattern to match PANs starting with the given prefix
            regex_pattern = (
                f"{value}.*"  # This regex pattern matches PANs starting with the value
            )
            schema_filters.append(Q("regexp", pan={"value": regex_pattern}))

        # Create a boolean query with "should" clauses for each regex pattern
        q &= Q("bool", should=schema_filters, minimum_should_match=1)
    if issuer_country:
        q &= Q(
            issuer_country__in=[
                str(issuer_country_value) for issuer_country_value in issuer_country
            ]
        )
    if pan:
        q &= Q(pan__iexact=pan)

    if transactionType:
        trans = []
        if type == "bespoke":
            for i in transactionType:
                trans.append(TransactionType.objects.filter(name=i)[0].bespokeCode)
        else:
            for i in transactionType:
                trans.append(TransactionType.objects.filter(name=i)[0].tlaCode)

        q &= Q(transaction_type__in=trans)
    if transactionStatus:
        if transactionStatus == "approved":
            if type == "bespoke":
                q &= Q(transaction_status__iexact="true")
            else:
                q &= Q(transaction_status__iexact="1")
        elif transactionStatus == "declined":
            if type == "bespoke":
                q &= ~Q(transaction_status__iexact="false")
            else:
                q &= ~Q(transaction_status__iexact="1")

    if domestic:
        if domestic == "domestic":
            q &= Q(acquirer_country=566)
        else:
            q &= ~Q(acquirer_country=566)
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

        if operator:
            lookup_field = f"amount{operator}"
            q &= Q(**{lookup_field: amount_value})
    if charge:
        if charge.lower() == "yes":
            values_to_check = allTransaction.values_list("rrn", flat=True)

            queryset_updms = UpDms.objects.using("etl_db").filter(
                trans_id__in=values_to_check
            )

            q &= Q(rrn__in=queryset_updms.values_list("trans_id", flat=True))

    if settlement:
        if settlement.lower() == "yes":
            values_to_check = allTransaction.values_list("rrn", flat=True)

            queryset_updms = SettlementDetail.objects.using("etl_db").filter(
                trannumber__in=values_to_check
            )

            q &= Q(rrn__in=queryset_updms.values_list("trannumber", flat=True))
    allTransaction: Transactions = Transactions.objects.using("etl_db").filter(
        q, department=type, transaction_time__range=(start_date, end_date)
    )

    if volume:
        allTransaction = allTransaction[: int(volume)]

    return True, allTransaction
