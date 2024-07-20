from django.db import connection, connections
from datetime import datetime, timedelta
import ast
from django.core.cache import cache
from django.conf import settings
from report.models import Transactions, UpDms, SettlementDetail
from account.models import Institution, Channel, ExtraParameters, TransactionType
from datacore.modules.utils import log_request

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
            except IndexError:
                result = False
            status, bespoke = issuer_filter(
                data, institu[0].bespokeCode, False, "bespoke"
            )
            status, processing = issuer_filter(
                data, institu[0].tlaCode, result, "processing"
            )
        else:
            log_request("==========>>>>> all institution processing")
            status, bespoke = issuer_filter(data, False, False, "bespoke")
            status, processing = issuer_filter(data, False, False, "processing")

    except Exception as e:
        return False, f"Please make sure the institution is provided: {e}"

    if not status:
        return False, f"Please check if the params sent are in an array{bespoke}"
    # Merge the results from bespoke and processing
    merge_data = sorted(bespoke + processing, key=lambda x: x[4], reverse=True)
    return True, merge_data


def acquire_report_db(data):
    institu = Institution.objects.filter(name=data.get("acquireInstitutionName", ""))
    issuer_institu = Institution.objects.filter(name=data.get("issuerName", ""))

    try:
        if institu.exists() or issuer_institu.exists():
            try:
                result = institu[0].tlaCode
            except IndexError:
                result = False

            status, bespoke = acquire_filter(
                data, institu[0].bespokeCode, False, "bespoke"
            )
            status, processing = acquire_filter(
                data, issuer_institu[0].acquireTlaCode, result, "processing"
            )
        else:
            log_request("==========>>>>> all institution processing")
            status, bespoke = acquire_filter(data, False, False, "bespoke")
            status, processing = acquire_filter(data, False, False, "processing")

    except Exception as e:
        return False, f"Please make sure the institutions are provided: {e}"

    if not status:
        return False, f"Please check if the params sent are in an array: {bespoke}"

    # Merge the results from bespoke and processing
    merge_data = sorted(bespoke + processing, key=lambda x: x[4], reverse=True)
    return True, merge_data


def COacquire_report_db(data):
    institu = Institution.objects.filter(name=data.get("acquireInstitutionName", ""))
    issuer_institu = Institution.objects.filter(name=data.get("issuerName", ""))

    try:
        if institu.exists() or issuer_institu.exists():
            try:
                result = institu[0].tlaCode
            except IndexError:
                result = False

            status, bespoke = co_acquire_filter(
                data, institu[0].bespokeCode, False, "bespoke"
            )
            status, processing = co_acquire_filter(
                data, issuer_institu[0].acquireTlaCode, result, "processing"
            )
        else:
            log_request("==========>>>>> all institution processing")
            status, bespoke = co_acquire_filter(data, False, False, "bespoke")
            status, processing = co_acquire_filter(data, False, False, "processing")

    except Exception as e:
        return False, f"Please make sure the institutions are provided: {e}"

    if not status:
        return False, f"Please check if the params sent are in an array: {bespoke}"

    # Merge the results from bespoke and processing
    merge_data = sorted(bespoke + processing, key=lambda x: x[4], reverse=True)
    return True, merge_data


def issuer_filter(data, name, acquire_id, type) -> dict:
    try:
        issuer_institution_name = name
        issuer_country = ast.literal_eval(data.get("countryCode", "[]"))
        account_currency = ast.literal_eval(data.get("accountCurrency", "[]"))
        schema = ast.literal_eval(data.get("shemaNAME", "[]"))
        currency = ast.literal_eval(data.get("transactionCurrency", "[]"))
        curreny_orig = ast.literal_eval(data.get("originCurrency", "[]"))
        pan = data.get("pan", None)
        bin = ast.literal_eval(data.get("issuerBin", "[]"))
        marchantId = data.get("marchantId", None)
        terminalId = data.get("terminalId", None)
        acquireBin = ast.literal_eval(data.get("acquireBin", "[]"))
        transaction = ast.literal_eval(data.get("acquireTransaction", "[]"))
        acquireCountry = ast.literal_eval(data.get("acquireCountryCode", "[]"))
        channel = ast.literal_eval(data.get("channel", "[]"))
        transactionType = ast.literal_eval(data.get("transactionType", "[]"))
        domestic = data.get("domestic", None)
        transactionStatus = data.get("transactionStatus", None)
        processing = data.get("processing", None)
        charge = data.get("charge", None)
        settlement = data.get("settlement", None)
        start_date = data.get("start", None)
        end_date = data.get("end", None)
        volume = data.get("volume", None)
        AmountValue = data.get("value", None)
    except Exception as e:
        log_request(
            f"unable to filter based on incorrect type in params  ===>> error {e}"
        )
        return False, "invalid params sent"

    # Convert end_date to a datetime object and add one day
    end_date = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)

    # Fetch UPBIN if needed
    if processing != "others":
        upbin = (
            ExtraParameters.objects.filter(name="UPBIN").first()
            if ExtraParameters.objects.filter(name="UPBIN").exists()
            else []
        )
    else:
        upbin = []

    # Combine bins
    totalbin = list(set(bin).union(set(acquireBin)))
    totalupbin = list(set(totalbin).union(set(upbin)))
    totalbin = [str(item) for item in totalupbin]
    totalCurrency = list(set(currency).union(set(transaction)))

    # Build the SQL query
    sql_query = """
        SELECT * FROM transactions
        WHERE department = %s
    """
    query_params = [type]

    if issuer_institution_name:
        sql_query += " AND issuer_institution_name ILIKE %s"
        query_params.append(issuer_institution_name)

    if schema:
        schema_filters = " OR ".join([f"pan LIKE %s" for _ in schema])
        sql_query += f" AND ({schema_filters})"
        query_params.extend([str(value) + "%" for value in schema])

    if issuer_country:
        sql_query += (
            " AND issuer_country IN (" + ", ".join(["%s"] * len(issuer_country)) + ")"
        )
        query_params.extend([str(item) for item in issuer_country])

    if account_currency:
        sql_query += (
            " AND account_curreny IN ("
            + ", ".join(["%s"] * len(account_currency))
            + ")"
        )
        query_params.extend(account_currency)

    if currency:
        sql_query += " AND currency IN (" + ", ".join(["%s"] * len(totalCurrency)) + ")"
        query_params.extend([str(item) for item in totalCurrency])

    if curreny_orig:
        sql_query += (
            " AND curreny_orig IN (" + ", ".join(["%s"] * len(curreny_orig)) + ")"
        )
        query_params.extend([str(item) for item in curreny_orig])

    if channel:
        if type == "bespoke":
            sql_query += " AND channel IN (" + ", ".join(["%s"] * len(channel)) + ")"
            query_params.extend(channel)
        else:
            channel_value = []
            if ["Web", "POS"] == channel or ["POS", "Web"] == channel:
                for i in channel:
                    data_tla = Channel.objects.filter(name=i)[0].tlaCode
                    channel_value.append(data_tla)
                sql_query += (
                    " AND channel IN (" + ", ".join(["%s"] * len(channel_value)) + ")"
                )
                query_params.extend(channel_value)
            elif ["Web"] == channel:
                value = Channel.objects.filter(name="Web")[0].tlaCode
                sql_query += " AND channel = %s AND secondary_channel IN %s"
                query_params.extend(
                    [value, (8, 81, 82, 83, 84, 85, 86, 87, 88, 89, 61, 62, 52, 59)]
                )
            else:
                for i in channel:
                    data_tla = Channel.objects.filter(name=i)[0].tlaCode
                    channel_value.append(data_tla)
                sql_query += (
                    " AND channel IN (" + ", ".join(["%s"] * len(channel_value)) + ")"
                )
                query_params.extend([str(item) for item in channel_value])

    if acquire_id:
        sql_query += " AND acquirer_institution_id IN ('1', %s)"
        query_params.append(acquire_id)

    if marchantId:
        sql_query += " AND merchant_id = %s"
        query_params.append(marchantId)

    if terminalId:
        sql_query += " AND aquirer_terminal_id = %s"
        query_params.append(terminalId)

    if transactionType:
        trans = []
        final_trans = []
        if type == "bespoke":
            for i in transactionType:
                trans.append(TransactionType.objects.filter(name=i)[0].bespokeCode)
            for tran in trans:
                final_trans += tran.split(",")
        else:
            for i in transactionType:
                trans.append(TransactionType.objects.filter(name=i)[0].tlaCode)
            for tran in trans:
                final_trans += tran.split(",")
        sql_query += (
            " AND transaction_type IN (" + ", ".join(["%s"] * len(final_trans)) + ")"
        )
        query_params.extend([str(item) for item in final_trans])

    if transactionStatus:
        if transactionStatus == "approved":
            status_value = "true" if type == "bespoke" else "1"
        elif transactionStatus == "declined":
            status_value = "false" if type == "bespoke" else "0"
        sql_query += " AND transaction_status = %s"
        query_params.append(status_value)

    if pan:
        sql_query += " AND pan = %s"
        query_params.append(pan)

    if totalbin:
        totalbin_filter = " OR ".join([f"pan LIKE %s" for _ in totalbin])
        sql_query += f" AND ({totalbin_filter})"
        query_params.extend([str(bin) + "%" for bin in totalbin])

    if acquireCountry:
        sql_query += (
            " AND acquirer_country IN (" + ", ".join(["%s"] * len(acquireCountry)) + ")"
        )
        query_params.extend([str(item) for item in acquireCountry])

    if start_date and end_date:
        sql_query += " AND transaction_time BETWEEN %s AND %s"
        query_params.extend([start_date, end_date])

    if domestic:
        if domestic == "domestic":
            sql_query += " AND acquirer_country = 566"
        else:
            sql_query += " AND acquirer_country != 566"

    if AmountValue:
        operator_mapping = {
            ">=": ">=",
            ">": ">",
            "<=": "<=",
            "<": "<",
            "=": "=",
        }

        operator = ""
        amount_value = AmountValue

        for op, lookup in operator_mapping.items():
            if AmountValue.endswith(op):
                operator = lookup
                amount_value = AmountValue[: -len(op)].rstrip()
                break

        if operator:
            sql_query += f" AND amount {operator} %s"
            query_params.append(amount_value)

    # Execute the initial query
    with connections["etl_db"].cursor() as cursor:
        cursor.execute(sql_query, query_params)
        allTransaction = cursor.fetchall()

    # Additional filtering for chargeback
    if charge and charge.lower() == "yes":
        rrn_list = [
            row[0] for row in allTransaction
        ]  # Assuming the first column is rrn
        sql_query = "SELECT trans_id FROM up_dms WHERE trans_id IN %s"
        query_params = [tuple(rrn_list)]

        with connections["etl_db"].cursor() as cursor:
            cursor.execute(sql_query, query_params)
            chargeback_rrns = [row[0] for row in cursor.fetchall()]

        allTransaction = [row for row in allTransaction if row[0] in chargeback_rrns]

    # Additional filtering for settlement
    if settlement and settlement.lower() == "yes":
        request_id_list = [
            row[0] for row in allTransaction
        ]  # Assuming the first column is request_id
        sql_query = "SELECT trannumber FROM settlement_detail WHERE trannumber IN %s"
        query_params = [tuple(request_id_list)]

        with connections["etl_db"].cursor() as cursor:
            cursor.execute(sql_query, query_params)
            settlement_rrns = [row[0] for row in cursor.fetchall()]

        allTransaction = [row for row in allTransaction if row[0] in settlement_rrns]

    # Apply volume limit
    if volume:
        allTransaction = allTransaction[: int(volume)]

    return True, allTransaction


def acquire_filter(data, name, issuer_name, type) -> dict:
    try:
        acquire_institution_name = name
        currency = ast.literal_eval(data.get("transactionCurrency", "[]"))
        curreny_orig = ast.literal_eval(data.get("originCurrency", "[]"))
        pan = data.get("pan", None)
        bin_list = ast.literal_eval(data.get("issuerBin", "[]"))
        issuer_country = ast.literal_eval(data.get("countryCode", "[]"))
        marchantId = data.get("marchantId", None)
        schema = ast.literal_eval(data.get("schema", "[]"))
        terminalId = data.get("terminalId", None)
        acquireBin = ast.literal_eval(data.get("acquireBin", "[]"))
        transaction = ast.literal_eval(data.get("acquireTransaction", "[]"))
        acquireCountry = ast.literal_eval(data.get("acquireCountryCode", "[]"))
        domestic = data.get("domestic", None)
        transactionStatus = data.get("transactionStatus", None)
        transactionType = ast.literal_eval(data.get("transactionType", "[]"))
        processing = data.get("processing", None)
        charge = data.get("charge", None)
        settlement = data.get("settlement", None)
        AmountValue = data.get("value", None)
        start_date = data.get("start", None)
        end_date = data.get("end", None)
        volume = data.get("volume", None)
    except Exception as e:
        log_request(f"Unable to filter due to incorrect parameters: {e}")
        return False, "Invalid parameters sent"

    # Convert end_date to a datetime object and add one day
    end_date = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)

    # Fetch UPBIN if needed
    if processing != "others":
        upbin = (
            ExtraParameters.objects.filter(name="UPBIN").first()
            if ExtraParameters.objects.filter(name="UPBIN").exists()
            else []
        )
    else:
        upbin = []

    # Combine bins
    totalbin = list(set(bin_list).union(set(acquireBin)))
    totalupbin = list(set(totalbin).union(set(upbin)))
    totalbin = [str(item) for item in totalupbin]
    totalCurrency = list(set(currency).union(set(transaction)))

    # Build the SQL query
    sql_query = """
        SELECT * FROM transactions
        WHERE department = %s AND transaction_time BETWEEN %s AND %s
    """
    query_params = [type, start_date, end_date]

    if acquire_institution_name:
        sql_query += " AND acquirer_institution_id IN ('1', %s)"
        query_params.append(acquire_institution_name)

    if totalbin:
        sql_query += " AND (" + " OR ".join(["pan LIKE %s" for _ in totalbin]) + ")"
        query_params.extend([str(bin) + "%" for bin in totalbin])

    if currency:
        sql_query += " AND currency IN (" + ", ".join(["%s"] * len(totalCurrency)) + ")"
        query_params.extend([str(item) for item in totalCurrency])

    if terminalId:
        sql_query += " AND aquirer_terminal_id = %s"
        query_params.append(terminalId)

    if marchantId:
        sql_query += " AND merchant_id = %s"
        query_params.append(marchantId)

    if curreny_orig:
        sql_query += (
            " AND curreny_orig IN (" + ", ".join(["%s"] * len(curreny_orig)) + ")"
        )
        query_params.extend([str(item) for item in curreny_orig])

    if acquireCountry:
        sql_query += (
            " AND acquirer_country IN (" + ", ".join(["%s"] * len(acquireCountry)) + ")"
        )
        query_params.extend([str(item) for item in acquireCountry])

    if issuer_name:
        sql_query += " AND issuer_institution_name = %s"
        query_params.append(issuer_name)

    if schema:
        schema_filters = " OR ".join([f"pan LIKE %s" for _ in schema])
        sql_query += f" AND ({schema_filters})"
        query_params.extend([str(value) + "%" for value in schema])

    if issuer_country:
        sql_query += (
            " AND issuer_country IN (" + ", ".join(["%s"] * len(issuer_country)) + ")"
        )
        query_params.extend([str(item) for item in issuer_country])

    if pan:
        sql_query += " AND pan = %s"
        query_params.append(pan)

    if transactionType:
        trans = []
        for i in transactionType:
            if type == "bespoke":
                trans.append(TransactionType.objects.filter(name=i)[0].bespokeCode)
            else:
                trans.append(TransactionType.objects.filter(name=i)[0].tlaCode)
        sql_query += " AND transaction_type IN (" + ", ".join(["%s"] * len(trans)) + ")"
        query_params.extend(trans)

    if transactionStatus:
        if transactionStatus == "approved":
            status_value = "true" if type == "bespoke" else "1"
            sql_query += " AND transaction_status = %s"
        elif transactionStatus == "declined":
            status_value = "false" if type == "bespoke" else "0"
            sql_query += " AND transaction_status != %s"
        query_params.append(status_value)

    if domestic:
        if domestic == "domestic":
            sql_query += " AND acquirer_country = 566"
        else:
            sql_query += " AND acquirer_country != 566"

    if AmountValue:
        operator_mapping = {
            ">=": ">=",
            ">": ">",
            "<=": "<=",
            "<": "<",
            "=": "=",
        }

        operator = ""
        amount_value = AmountValue

        for op, lookup in operator_mapping.items():
            if AmountValue.endswith(op):
                operator = lookup
                amount_value = AmountValue[: -len(op)].rstrip()
                break

        if operator:
            sql_query += f" AND amount {operator} %s"
            query_params.append(amount_value)
    print(sql_query, query_params)
    # Execute the initial query
    with connections["etl_db"].cursor() as cursor:
        cursor.execute(sql_query, query_params)
        allTransaction = cursor.fetchall()

    # Additional filtering for chargeback
    if charge and charge.lower() == "yes":
        rrn_list = [
            row[0] for row in allTransaction
        ]  # Assuming the first column is rrn
        sql_query = "SELECT trans_id FROM up_dms WHERE trans_id IN %s"
        query_params = [tuple(rrn_list)]

        with connections["etl_db"].cursor() as cursor:
            cursor.execute(sql_query, query_params)
            chargeback_rrns = [row[0] for row in cursor.fetchall()]

        allTransaction = [row for row in allTransaction if row[0] in chargeback_rrns]

    # Additional filtering for settlement
    if settlement and settlement.lower() == "yes":
        request_id_list = [
            row[0] for row in allTransaction
        ]  # Assuming the first column is request_id
        sql_query = "SELECT trannumber FROM settlement_detail WHERE trannumber IN %s"
        query_params = [tuple(request_id_list)]

        with connections["etl_db"].cursor() as cursor:
            cursor.execute(sql_query, query_params)
            settlement_rrns = [row[0] for row in cursor.fetchall()]

        allTransaction = [row for row in allTransaction if row[0] in settlement_rrns]

    # Apply volume limit
    if volume:
        allTransaction = allTransaction[: int(volume)]

    return True, allTransaction


def co_acquire_filter(data, name, issuer_name, type) -> dict:
    try:
        acquire_institution_name = name
        currency = ast.literal_eval(data.get("transactionCurrency", "[]"))
        curreny_orig = ast.literal_eval(data.get("originCurrency", "[]"))
        pan = data.get("pan", None)
        bin_list = ast.literal_eval(data.get("issuerBin", "[]"))
        issuer_country = ast.literal_eval(data.get("countryCode", "[]"))
        marchantId = data.get("marchantId", None)
        schema = ast.literal_eval(data.get("schema", "[]"))
        terminalId = data.get("terminalId", None)
        acquireBin = ast.literal_eval(data.get("acquireBin", "[]"))
        transaction = ast.literal_eval(data.get("acquireTransaction", "[]"))
        acquireCountry = ast.literal_eval(data.get("acquireCountryCode", "[]"))
        domestic = data.get("domestic", None)
        transactionStatus = data.get("transactionStatus", None)
        transactionType = ast.literal_eval(data.get("transactionType", "[]"))
        processing = data.get("processing", None)
        charge = data.get("charge", None)
        settlement = data.get("settlement", None)
        AmountValue = data.get("value", None)
        start_date = data.get("start", None)
        end_date = data.get("end", None)
        volume = data.get("volume", None)
    except Exception as e:
        log_request(f"Unable to filter due to incorrect parameters: {e}")
        return False, "Invalid parameters sent"

    # Convert end_date to a datetime object and add one day
    end_date = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)

    # Fetch UPBIN if needed
    if processing != "others":
        upbin = (
            ExtraParameters.objects.filter(name="UPBIN").first()
            if ExtraParameters.objects.filter(name="UPBIN").exists()
            else []
        )
    else:
        upbin = []

    # Combine bins
    totalbin = list(set(bin_list).union(set(acquireBin)))
    totalupbin = list(set(totalbin).union(set(upbin)))
    totalbin = [str(item) for item in totalupbin]
    totalCurrency = list(set(currency).union(set(transaction)))

    # Build the SQL query
    sql_query = """
        SELECT * FROM transactions
        WHERE department = %s AND transaction_time BETWEEN %s AND %s
    """
    query_params = [type, start_date, end_date]

    if acquire_institution_name:
        sql_query += " AND acquirer_institution_id IN ('1', %s)"
        query_params.append(str(acquire_institution_name))

    if totalbin:
        sql_query += " AND (" + " OR ".join(["pan LIKE %s" for _ in totalbin]) + ")"
        query_params.extend([bin + "%" for bin in totalbin])

    if currency:
        sql_query += " AND currency IN (" + ", ".join(["%s"] * len(totalCurrency)) + ")"
        query_params.extend(totalCurrency)

    if terminalId:
        sql_query += " AND aquirer_terminal_id = %s"
        query_params.append(terminalId)

    if marchantId:
        sql_query += " AND merchant_id = %s"
        query_params.append(marchantId)

    if curreny_orig:
        sql_query += (
            " AND curreny_orig IN (" + ", ".join(["%s"] * len(curreny_orig)) + ")"
        )
        query_params.extend([str(item) for item in curreny_orig])

    if acquireCountry:
        sql_query += (
            " AND acquirer_country IN (" + ", ".join(["%s"] * len(acquireCountry)) + ")"
        )
        query_params.extend([str(item) for item in acquireCountry])

    if issuer_name:
        sql_query += " AND issuer_institution_name = %s"
        query_params.append(issuer_name)

    if schema:
        schema_filters = " OR ".join([f"pan LIKE %s" for _ in schema])
        sql_query += f" AND ({schema_filters})"
        query_params.extend([str(value) + "%" for value in schema])

    if issuer_country:
        sql_query += (
            " AND issuer_country IN (" + ", ".join(["%s"] * len(issuer_country)) + ")"
        )
        query_params.extend([str(item) for item in issuer_country])

    if pan:
        sql_query += " AND pan = %s"
        query_params.append(pan)

    if transactionType:
        trans = []
        for i in transactionType:
            if type == "bespoke":
                trans.append(TransactionType.objects.filter(name=i)[0].bespokeCode)
            else:
                trans.append(TransactionType.objects.filter(name=i)[0].tlaCode)
        sql_query += " AND transaction_type IN (" + ", ".join(["%s"] * len(trans)) + ")"
        query_params.extend(trans)

    if transactionStatus:
        if transactionStatus == "approved":
            status_value = "true" if type == "bespoke" else "1"
            sql_query += " AND transaction_status = %s"
        elif transactionStatus == "declined":
            status_value = "false" if type == "bespoke" else "0"
            sql_query += " AND transaction_status != %s"
        query_params.append(status_value)

    if domestic:
        if domestic == "domestic":
            sql_query += " AND acquirer_country = '566' "
        else:
            sql_query += " AND acquirer_country != '566' "

    if AmountValue:
        operator_mapping = {
            ">=": ">=",
            ">": ">",
            "<=": "<=",
            "<": "<",
            "=": "=",
        }

        operator = ""
        amount_value = AmountValue

        for op, lookup in operator_mapping.items():
            if AmountValue.endswith(op):
                operator = lookup
                amount_value = AmountValue[: -len(op)].rstrip()
                break

        if operator:
            sql_query += f" AND amount {operator} %s"
            query_params.append(amount_value)

    # Execute the initial query
    with connections["etl_db"].cursor() as cursor:
        cursor.execute(sql_query, query_params)
        allTransaction = cursor.fetchall()

    # Additional filtering for chargeback
    if charge and charge.lower() == "yes":
        rrn_list = [
            row[0] for row in allTransaction
        ]  # Assuming the first column is rrn
        sql_query = "SELECT trans_id FROM up_dms WHERE trans_id IN %s"
        query_params = [tuple(rrn_list)]

        with connections["etl_db"].cursor() as cursor:
            cursor.execute(sql_query, query_params)
            chargeback_rrns = [row[0] for row in cursor.fetchall()]

        allTransaction = [row for row in allTransaction if row[0] in chargeback_rrns]

    # Additional filtering for settlement
    if settlement and settlement.lower() == "yes":
        request_id_list = [
            row[0] for row in allTransaction
        ]  # Assuming the first column is request_id
        sql_query = "SELECT trannumber FROM settlement_detail WHERE trannumber IN %s"
        query_params = [tuple(request_id_list)]

        with connections["etl_db"].cursor() as cursor:
            cursor.execute(sql_query, query_params)
            settlement_rrns = [row[0] for row in cursor.fetchall()]

        allTransaction = [row for row in allTransaction if row[0] in settlement_rrns]

    # Apply volume limit
    if volume:
        allTransaction = allTransaction[: int(volume)]

    return True, allTransaction
