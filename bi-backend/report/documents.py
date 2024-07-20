from django_elasticsearch_dsl import Document, fields
from django_elasticsearch_dsl.registries import registry
from .models import *


#
# @registry.register_document
# class BankBranchEtlDocument(Document):
#     class Index:
#         name = 'bank_branch_etl_index'
#         settings = {'number_of_shards': 2, 'number_of_replicas': 1}
#
#     class Django:
#         model = BankBranchEtl
#
#     department = fields.TextField()
#     applicant_name = fields.TextField()
#     schema_or_table_name = fields.TextField()
#     op_date = fields.DateField()
#     gid = fields.KeywordField()
#     item_no = fields.KeywordField()
#     tran_no = fields.KeywordField()
#     app_type = fields.TextField()
#     tran_amount = fields.FloatField()
#     app_id = fields.KeywordField()
#     tran_date = fields.DateField()
#     full_rem = fields.TextField()
#     payment_mode = fields.TextField()
#     response_code = fields.KeywordField()
#     state_code = fields.KeywordField()
#     state_name = fields.TextField()
#     approval_code = fields.KeywordField()
#     bank_name = fields.TextField()
#     bank_code = fields.KeywordField()
#     account_name_bank = fields.TextField()
#     retailer_id = fields.KeywordField()
#     term_id = fields.KeywordField()
#     tran_currency = fields.TextField()
#     branch_code = fields.KeywordField()
#     payment_status = fields.TextField()
#     reference_no = fields.KeywordField()
#     settled_status = fields.TextField()
#     deposit_slip_no = fields.KeywordField()
#     response = fields.TextField()
#
@registry.register_document
class CardAccountDetailsIssuingDocument(Document):
    class Index:
        name = "tcard_index"
        settings = {"number_of_shards": 1, "number_of_replicas": 1}

    class Django:
        model = CardAccountDetailsIssuing

    account_no = fields.TextField()
    account_type = fields.TextField()
    acct_stat = fields.TextField()
    acct_typ = fields.TextField()
    available = fields.TextField()
    branch = fields.FloatField()
    client_id = fields.TextField()
    create_clerk_code = fields.TextField()
    credit_reserve = fields.TextField()
    currency_no = fields.TextField()
    debit_reserve = fields.TextField()
    department = fields.TextField()
    existsinpc = fields.TextField()
    id = fields.LongField()
    limit_grp_id = fields.TextField()
    lower_main = fields.TextField()
    over_draft = fields.TextField()
    remain_update_date = fields.DateField()
    remark = fields.TextField()
    schema_or_table_name = fields.TextField()
    taccount_createdate = fields.DateField()
    taccount_updatedate = fields.DateField()
    update_clerk_code = fields.TextField()
    update_revision = fields.TextField()
    update_sys_date = fields.DateField()


#
#
# @registry.register_document
# class CardClientPersonnelIssuingDocument(Document):
#     class Index:
#         name = 'card_client_personnel_issuing_index'
#         settings = {'number_of_shards': 2, 'number_of_replicas': 1}
#
#     class Django:
#         model = CardClientPersonnelIssuing
#
#     department = fields.TextField()
#     branch = fields.TextField()
#     client_id = fields.TextField()
#     country_res = fields.TextField()
#     sex = fields.TextField()
#     birthday = fields.DateField()
#     birth_place = fields.TextField()
#     foreign_pass = fields.TextField()
#     foreign_exp = fields.DateField()
#     country_reg = fields.TextField()
#     region_reg = fields.TextField()
#     city_reg = fields.TextField()
#     zip_reg = fields.TextField()
#     address_reg = fields.TextField()
#     country_live = fields.TextField()
#     region_live = fields.TextField()
#     city_live = fields.TextField()
#     zip_live = fields.TextField()
#     address = fields.TextField()
#     country_cont = fields.TextField()
#     region_cont = fields.TextField()
#     city_cont = fields.TextField()
#     zip_cont = fields.TextField()
#     address_cont = fields.TextField()
#     phone = fields.TextField()
#     fax = fields.TextField()
#     email = fields.TextField()
#     mobile_phone = fields.TextField()
#     pager = fields.TextField()
#     company = fields.TextField()
#     office = fields.TextField()
#     tab_no = fields.TextField()
#     job_title = fields.TextField()
#     job_phone = fields.TextField()
#     date_create = fields.DateField()
#     date_modify = fields.DateField()
#     occupation = fields.TextField()
#     vip = fields.TextField()
#     insider = fields.TextField()
#     resident = fields.TextField()
#     title = fields.TextField()
#     street_reg = fields.TextField()
#     house_reg = fields.TextField()
#     death_date = fields.DateField()
#     first_name = fields.TextField()
#     middle_name = fields.TextField()
#     last_name = fields.TextField()
#     category_id = fields.TextField()
#     grp_limit = fields.TextField()
#     grp_limit_int = fields.TextField()
#     limit_currency = fields.TextField()
#     schema_or_table_name = fields.TextField()
#     bankrupt = fields.TextField()
#
#
# @registry.register_document
# class CipaPartiesMerchantDocument(Document):
#     class Index:
#         name = 'cipapartiesmerchant_index'
#         settings = {'number_of_shards': 2, 'number_of_replicas': 1}
#
#     class Django:
#         model = CipaPartiesMerchant
#
#     code = fields.TextField()
#     name = fields.TextField()
#     bank = fields.TextField()
#     account = fields.TextField()
#     email = fields.TextField()
#     isenabled = fields.TextField()
#     ismerchant = fields.TextField()
#     url = fields.TextField()
#     profileid = fields.TextField()
#     feeincluded = fields.TextField()
#     secretkey = fields.TextField()
#     ipaddresses = fields.TextField()
#     merchantid = fields.TextField()
#     merid = fields.TextField()
#     profilename = fields.TextField()
#     terminalid = fields.TextField()
#     discriminator = fields.TextField()
#     retailerid = fields.TextField()
#     migrated = fields.TextField()
#     amexid = fields.TextField()
#     iscardenabled = fields.TextField()
#     isrecurring = fields.TextField()
#     payattitudefee = fields.TextField()
#     cardfee = fields.TextField()
#     maxfee = fields.TextField()
#     vat = fields.TextField()
#     addvat = fields.TextField()
#     encryptionkey = fields.TextField()
#     encryptioniv = fields.TextField()
#     reversalenabled = fields.TextField()
#     enablecardonfile = fields.TextField()
#     cardonfileref = fields.TextField()
#     cardonfileid = fields.TextField()
#     ordertimeout = fields.TextField()
#     disablepayattitude = fields.TextField()
#
#
#
# @registry.register_document
# class PayarenaExchangeDocument(Document):
#     class Index:
#         name = 'payarenaexchange_index'
#         settings = {'number_of_shards': 2, 'number_of_replicas': 1}
#     class Django:
#         model = PayarenaExchange
#
#     department = fields.TextField()
#     schema_or_table_name = fields.TextField()
#     request_time = fields.DateField()
#     response_time = fields.DateField()
#     account_no = fields.TextField()
#     amount = fields.TextField()
#     request_id = fields.TextField()
#     request_type = fields.TextField()
#     route = fields.TextField()
#     status_code = fields.TextField()
#     status_message = fields.TextField()
#     response_id = fields.TextField()
#     rrn = fields.TextField()
#     client = fields.TextField()
#     reversed = fields.TextField()
#     reversible = fields.TextField()
#     batch_id = fields.TextField()
#     beneficiary = fields.TextField()
#     sender = fields.TextField()
#     reference = fields.TextField()
#     channel = fields.TextField()
#     source_bank = fields.TextField()
#     txid = fields.TextField()
#     dest_bank = fields.TextField()
#
#
# @registry.register_document
# class ProcessRecordDocument(Document):
#     class Index:
#         name = 'processrecord_index'
#         settings = {'number_of_shards': 2, 'number_of_replicas': 1}
#
#     class Django:
#         model = ProcessRecord
#
#     script_name = fields.TextField()
#     record = fields.IntegerField()
#     last_date_ran = fields.DateField()
#
#
#
# @registry.register_document
# class SettlementMebDocument(Document):
#     class Index:
#         name = 'settlement_meb_index'
#         settings = {'number_of_shards': 2, 'number_of_replicas': 1}
#
#     class Django:
#         model = SettlementMeb
#
#     department_table_id = fields.KeywordField(attr='department_table_id')
#     department = fields.KeywordField(attr='department')
#     opdate = fields.DateField(attr='opdate')
#     clerkcode = fields.KeywordField(attr='clerkcode')
#     station = fields.KeywordField(attr='station')
#     docno = fields.KeywordField(attr='docno')
#     no = fields.KeywordField(attr='no')
#     entcode = fields.KeywordField(attr='entcode')
#     debitacct = fields.KeywordField(attr='debitacct')
#     debitval = fields.KeywordField(attr='debitval')
#     creditacct = fields.KeywordField(attr='creditacct')
#     creditval = fields.KeywordField(attr='creditval')
#     shortrem = fields.KeywordField(attr='shortrem')
#     fullrem = fields.KeywordField(attr='fullrem')
#     device = fields.KeywordField(attr='device')
#     extentcode = fields.KeywordField(attr='extentcode')
#     trandate = fields.KeywordField(attr='trandate')
#     issficode = fields.KeywordField(attr='issficode')
#     issfiid = fields.KeywordField(attr='issfiid')
#     isscountry = fields.KeywordField(attr='isscountry')
#     pan = fields.KeywordField(attr='pan')
#     mbr = fields.KeywordField(attr='mbr')
#     expdate = fields.DateField(attr='expdate')
#     approval = fields.KeywordField(attr='approval')
#     acqficode = fields.KeywordField(attr='acqficode')
#     acqfiid = fields.KeywordField(attr='acqfiid')
#     acqps = fields.KeywordField(attr='acqps')
#     acqcountry = fields.KeywordField(attr='acqcountry')
#     termcode = fields.KeywordField(attr='termcode')
#     term = fields.KeywordField(attr='term')
#     retailer = fields.KeywordField(attr='retailer')
#     tlocation = fields.KeywordField(attr='tlocation')
#     mcc = fields.KeywordField(attr='mcc')
#     stan = fields.KeywordField(attr='stan')
#     acqiin = fields.KeywordField(attr='acqiin')
#     issps = fields.KeywordField(attr='issps')
#     forwardiin = fields.KeywordField(attr='forwardiin')
#     invoice = fields.KeywordField(attr='invoice')
#     userdata = fields.KeywordField(attr='userdata')
#     tranamount = fields.KeywordField(attr='tranamount')
#     trancur = fields.KeywordField(attr='trancur')
#     orgamount = fields.KeywordField(attr='orgamount')
#     orgcur = fields.KeywordField(attr='orgcur')
#     acqfee = fields.KeywordField(attr='acqfee')
#     acqfeecur = fields.KeywordField(attr='acqfeecur')
#     issfee = fields.KeywordField(attr='issfee')
#     issfeecur = fields.KeywordField(attr='issfeecur')
#     intrfee = fields.KeywordField(attr='intrfee')
#     intrfeecur = fields.KeywordField(attr='intrfeecur')
#     ident1 = fields.KeywordField(attr='ident1')
#     info1 = fields.KeywordField(attr='info1')
#     ident2 = fields.KeywordField(attr='ident2')
#     info2 = fields.KeywordField(attr='info2')
#     ident3 = fields.KeywordField(attr='ident3')
#     info3 = fields.KeywordField(attr='info3')
#     debitcur = fields.KeywordField(attr='debitcur')
#     creditcur = fields.KeywordField(attr='creditcur')
#     crnum = fields.KeywordField(attr='crnum')
#     bifee = fields.KeywordField(attr='bifee')
#     usdfn = fields.KeywordField(attr='usdfn')
#     usdfn2 = fields.KeywordField(attr='usdfn2')
#     usdfn3 = fields.KeywordField(attr='usdfn3')
#     process_status = fields.KeywordField(attr='process_status')
#     remark = fields.KeywordField(attr='remark')
#     recordid = fields.KeywordField(attr='recordid')
#     transign = fields.KeywordField(attr='transign')
#     batchno = fields.KeywordField(attr='batchno')
#     code = fields.KeywordField(attr='code')
#     opdate2 = fields.DateField(attr='opdate2')
#     createdate = fields.DateField(attr='createdate')
#     info1_temp = fields.KeywordField(attr='info1_temp')
#
#
@registry.register_document
class SettlementDetailDocument(Document):
    class Index:
        name = "settlement_detail_index"
        settings = {"number_of_shards": 1, "number_of_replicas": 1}

    class Django:
        model = SettlementDetail
        fields = ["docno"]


@registry.register_document
class SettlementMebDocument(Document):
    class Index:
        name = "settlement_meb_index"
        settings = {"number_of_shards": 1, "number_of_replicas": 1}

    class Django:
        model = SettlementMeb

    docno = fields.KeywordField(attr="docno")
    trandate = fields.KeywordField(attr="trandate")
    pan = fields.KeywordField(attr="pan")
    posentrymode = fields.KeywordField(attr="posentrymode")
    origtime = fields.DateField(attr="origtime")
    packno = fields.KeywordField(attr="packno")
    tranamount = fields.KeywordField(attr="tranamount")
    opdate = fields.DateField(attr="opdate")
    createdate = fields.DateField(attr="createdate")
    time = fields.DateField(attr="time")


@registry.register_document
class TransactionsDocument(Document):
    class Index:
        name = "transaction_index"
        settings = {"number_of_shards": 1, "number_of_replicas": 1}

    class Django:
        model = Transactions

    acquirer = fields.TextField(
        attr="acquirer", fields={"keyword": fields.KeywordField(ignore_above=256)}
    )
    acquirer_institution_id = fields.TextField(
        attr="acquirer_institution_id",
        fields={"keyword": fields.KeywordField(ignore_above=256)},
    )
    amount = fields.FloatField(attr="amount")
    beneficiary_bank = fields.TextField(
        attr="beneficiary_bank",
        fields={"keyword": fields.KeywordField(ignore_above=256)},
    )
    beneficiary_name = fields.TextField(
        attr="beneficiary_name",
        fields={"keyword": fields.KeywordField(ignore_above=256)},
    )
    channel = fields.TextField(
        attr="channel", fields={"keyword": fields.KeywordField(ignore_above=256)}
    )
    currency = fields.TextField(
        attr="currency", fields={"keyword": fields.KeywordField(ignore_above=256)}
    )
    department = fields.TextField(
        attr="department", fields={"keyword": fields.KeywordField(ignore_above=256)}
    )
    description = fields.TextField(
        attr="description", fields={"keyword": fields.KeywordField(ignore_above=256)}
    )
    fee = fields.FloatField(attr="fee")
    from_account = fields.TextField(
        attr="from_account", fields={"keyword": fields.KeywordField(ignore_above=256)}
    )
    id = fields.LongField(attr="id")
    is_reversal = fields.TextField(
        attr="is_reversal", fields={"keyword": fields.KeywordField(ignore_above=256)}
    )
    issuer = fields.TextField(
        attr="issuer", fields={"keyword": fields.KeywordField(ignore_above=256)}
    )
    issuer_institution_id = fields.TextField(
        attr="issuer_institution_id",
        fields={"keyword": fields.KeywordField(ignore_above=256)},
    )
    issuer_institution_name = fields.TextField(
        attr="issuer_institution_id",
        fields={"keyword": fields.KeywordField(ignore_above=256)},
    )
    narration = fields.TextField(
        attr="narration", fields={"keyword": fields.KeywordField(ignore_above=256)}
    )
    pan = fields.TextField(
        attr="pan", fields={"keyword": fields.KeywordField(ignore_above=256)}
    )
    rrn = fields.TextField(
        attr="rrn", fields={"keyword": fields.KeywordField(ignore_above=256)}
    )
    sender_account = fields.TextField(
        attr="sender_account", fields={"keyword": fields.KeywordField(ignore_above=256)}
    )
    sender_bank = fields.TextField(
        attr="sender_bank", fields={"keyword": fields.KeywordField(ignore_above=256)}
    )
    sender_name = fields.TextField(
        attr="sender_name", fields={"keyword": fields.KeywordField(ignore_above=256)}
    )
    status_code = fields.TextField(
        attr="status_code", fields={"keyword": fields.KeywordField(ignore_above=256)}
    )
    to_account = fields.TextField(
        attr="to_account", fields={"keyword": fields.KeywordField(ignore_above=256)}
    )
    total_amount = fields.FloatField(attr="total_amount")
    transaction_number = fields.TextField(
        attr="transaction_number",
        fields={"keyword": fields.KeywordField(ignore_above=256)},
    )
    transaction_status = fields.TextField(
        attr="transaction_status",
        fields={"keyword": fields.KeywordField(ignore_above=256)},
    )
    transaction_time = fields.DateField(attr="transaction_time")
    transaction_type = fields.TextField(
        attr="transaction_type",
        fields={"keyword": fields.KeywordField(ignore_above=256)},
    )
    account_curreny = fields.TextField(
        attr="acquirer", fields={"keyword": fields.KeywordField(ignore_above=256)}
    )
    acquirer_country = fields.TextField(
        attr="acquirer", fields={"keyword": fields.KeywordField(ignore_above=256)}
    )
    issuer_country = fields.TextField(
        attr="acquirer", fields={"keyword": fields.KeywordField(ignore_above=256)}
    )


# @registry.register_document
# class TransactionsDocument(Document):
#     class Index:
#         name = "transaction_index"
#         settings = {"number_of_shards": 2, "number_of_replicas": 1}

#     class Django:
#         model = Transactions

#     bespoke_name = fields.TextField(attr="transaction_type__bespoke_name")
#     bespoke_code = fields.TextField(attr="transaction_type__bespoke_code")
#     processing_code = fields.IntegerField(attr="transaction_type__processing_code")
#     processing_name = fields.TextField(attr="transaction_type__processing_name")
#     processing_constant_name = fields.TextField(
#         attr="transaction_type__processing_constant_name"
#     )
#     processing_class = fields.TextField(attr="transaction_type__processing_class")

#     department_table_id = fields.KeywordField(attr="department_table_id")
#     department = fields.TextField(attr="department")
#     transaction_time = fields.DateField(attr="transaction_time")
#     original_time = fields.DateField(attr="original_time")
#     channel = fields.TextField(attr="channel")
#     secondary_channel = fields.TextField(attr="secondary_channel")
#     aquirer_terminal_id = fields.KeywordField(attr="aquirer_terminal_id")
#     acquirer_ps_name = fields.TextField(attr="acquirer_ps_name")
#     acquirer_institution_id = fields.KeywordField(attr="acquirer_institution_id")
#     beneficiary_bank = fields.TextField(attr="beneficiary_bank")
#     beneficiary_name = fields.TextField(attr="beneficiary_name")
#     acquirer_country = fields.TextField(attr="acquirer_country")
#     acquirer_term_state = fields.TextField(attr="acquirer_term_state")
#     acquirer_term_city = fields.TextField(attr="acquirer_term_city")
#     term_owner = fields.TextField(attr="term_owner")
#     term_entry_caps = fields.TextField(attr="term_entry_caps")
#     from_account = fields.TextField(attr="from_account")
#     to_account = fields.TextField(attr="to_account")
#     to_date_field = fields.DateField(attr="to_date_field")
#     from_date = fields.DateField(attr="from_date")
#     pan = fields.KeywordField(attr="pan")
#     bonus = fields.TextField(attr="bonus")
#     currency = fields.KeywordField(attr="currency")
#     error = fields.TextField(attr="error")
#     proc_duration = fields.TextField(attr="proc_duration")
#     account_id = fields.TextField(attr="account_id")
#     phone = fields.KeywordField(attr="phone")
#     bank = fields.TextField(attr="bank")
#     trx_type = fields.TextField(attr="trx_type")
#     terminal_id = fields.KeywordField(attr="terminal_id")
#     tag = fields.TextField(attr="tag")
#     auth_duration = fields.TextField(attr="auth_duration")
#     transaction_number = fields.KeywordField(attr="transaction_number")
#     rrn = fields.KeywordField(attr="rrn")
#     issuer_country = fields.TextField(attr="issuer_country")
#     issuer_institution_id = fields.KeywordField(attr="issuer_institution_id")
#     card_type = fields.TextField(attr="card_type")
#     issuer_institution_name = fields.TextField(attr="issuer_institution_name")
#     account_curreny = fields.KeywordField(attr="account_curreny")
#     curreny_orig = fields.TextField(attr="curreny_orig")
#     amount = fields.FloatField(attr="amount")
#     transaction_status = fields.TextField(attr="transaction_status")
#     status = fields.TextField(attr="status")
#     status_code = fields.TextField(attr="status_code")
#     tran_category = fields.TextField(attr="tran_category")
#     merchant_id = fields.KeywordField(attr="merchant_id")
#     terminal_retailer = fields.TextField(attr="terminal_retailer")
#     auth_ps_name = fields.TextField(attr="auth_ps_name")
#     pos_entry_mode = fields.TextField(attr="pos_entry_mode")
#     description = fields.TextField(attr="description")
#     stan = fields.KeywordField(attr="stan")
#     fee = fields.FloatField(attr="fee")
#     total_amount = fields.FloatField(attr="total_amount")
#     merchant_fee = fields.FloatField(attr="merchant_fee")
#     merchant_account = fields.TextField(attr="merchant_account")
#     direction = fields.TextField(attr="direction")
#     authorization_ref = fields.KeywordField(attr="authorization_ref")
#     completed = fields.TextField(attr="completed")
#     narration = fields.TextField(attr="narration")
#     beneficiary = fields.TextField(attr="beneficiary")
#     sender_name = fields.TextField(attr="sender_name")
#     sender_bank = fields.TextField(attr="sender_bank")
#     sender_account = fields.TextField(attr="sender_account")
#     is_offline = fields.TextField(attr="is_offline")
#     issuer = fields.TextField(attr="issuer")
#     acquirer = fields.TextField(attr="acquirer")
#     location_field = fields.TextField(attr="location_field")
#     reversal_transaction_id = fields.KeywordField(attr="reversal_transaction_id")
#     request_id = fields.KeywordField(attr="request_id")
#     actual_date = fields.DateField(attr="actual_date")
#     reversal_date = fields.DateField(attr="reversal_date")
#     is_reversal = fields.TextField(attr="is_reversal")
#     reversed_transaction_id = fields.KeywordField(attr="reversed_transaction_id")
#     reversal_reason = fields.TextField(attr="reversal_reason")
#     proc_code = fields.TextField(attr="proc_code")
#     product_code = fields.TextField(attr="product_code")
#     accesslog_id = fields.KeywordField(attr="accesslog_id")
#     mcc = fields.KeywordField(attr="mcc")
#     terminal_date = fields.DateField(attr="terminal_date")
#     agent = fields.TextField(attr="agent")
#     balance = fields.TextField(attr="balance")
#     ip = fields.KeywordField(attr="ip")
#     beneficiary_uan = fields.TextField(attr="beneficiary_uan")
#     reference_trx_id = fields.KeywordField(attr="reference_trx_id")
#     holder_id = fields.TextField(attr="holder_id")
#     is_payment_request = fields.TextField(attr="is_payment_request")
#     receipt = fields.TextField(attr="receipt")
#     depositor = fields.TextField(attr="depositor")
#     super_agent_commission = fields.TextField(attr="super_agent_commission")
#     sub_agent_commission = fields.TextField(attr="sub_agent_commission")
#     agent_id = fields.TextField(attr="agent_id")
#     agent_code = fields.TextField(attr="agent_code")
#     super_agent_code = fields.TextField(attr="super_agent_code")
#     debit_code = fields.TextField(attr="debit_code")
#     credit_code = fields.TextField(attr="credit_code")
#     credit_route = fields.TextField(attr="credit_route")
#     issuer_fee = fields.FloatField(attr="issuer_fee")
#     debit_route = fields.TextField(attr="debit_route")
#     ussd_fee = fields.FloatField(attr="ussd_fee")
#     ussd_network = fields.TextField(attr="ussd_network")


#
# @registry.register_document
# class TransactionTypeDocument(Document):
#     class Index:
#         name = 'transaction_type_index'
#         settings = {'number_of_shards': 2, 'number_of_replicas': 1}
#
#     class Django:
#         model = TransactionType
#     bespoke_name = fields.TextField(attr='bespoke_name')
#     bespoke_code = fields.TextField(attr='bespoke_code')
#     processing_code = fields.IntegerField(attr='processing_code')
#     processing_name = fields.TextField(attr='processing_name')
#     processing_constant_name = fields.TextField(attr='processing_constant_name')
#     processing_class = fields.TextField(attr='processing_class')
#
# @registry.register_document
# class UpConTerminalConfigDocument(Document):
#     class Index:
#         name = 'up_con_terminal_config_index'
#         settings = {'number_of_shards': 2, 'number_of_replicas': 1}
#
#     class Django:
#         model = UpConTerminalConfig
#
#         fields = [
#                 "department_table_id", "department", "schema_or_table_name", "tid",
#                 "mid", "serial_number", "contact_title", "contact_name",
#                 "init_application_version", "merchant_name", "profile_name",
#                 "merchant_address", "contact_phone", "email", "state_code",
#                 "date_created", "bank_accno", "lga", "app_name",
#                 "terminal_owner_code", "terminal_manufacturer", "terminal_model",
#                 "added_by", "if_available", "tid_pin", "physical_address",
#                 "merchant_cat_code", "ptsp", "mechant_account_name", "bank_code",
#                 "name_created", "date_modified", "name_modified", "email_alerts",
#                 "visa_acquirer_id_number", "master_card_acquirer_id_number",
#                 "verve_acquirer_id_number", "account_name", "terminal_type",
#                 "country", "country_code", "payattitude_acquirer_id_number",
#                 "cup_acquirer_id_number", "jcb_acquirer_id_number", "maker",
#                 "checker", "bank_name", "term_ps_name", "payarenam_id",
#                 "payarena_tid", "comm_sid", "timestamp_field", "terminal_model_code",
#                 "pan", "proc_code", "rrn", "stan", "message_date",
#                 "message_started_date", "resp_code", "auth_code",
#                 "message_date_completed", "amount", "currency", "source",
#                 "destination", "hostrespcode"
#             ]
#
#
#
# @registry.register_document
# class UpDmsocument(Document):
#     class Index:
#         name = 'updms_index'
#         settings = {'number_of_shards': 2, 'number_of_replicas': 1}
#
#     class Django:
#         model = UpDms
#
#         fields = [
#             "department_table_id", "department", "schema_or_table_name",
#             "terminal_id", "trans_id", "dispute_id", "approval_code",
#             "dispute_status", "cpd", "location", "trans_amount",
#             "trans_date", "trans_time", "masked_pan", "acct_no",
#             "trans_type", "response", "issuer", "acquirer",
#             "issuer_stan", "acquirer_stan", "acquirer_rrn", "issuer_rrn",
#             "order_id", "settlement_date", "dispute_date", "due_date",
#             "dispense_amount", "log_comment", "full_access",
#             "reversal_id", "reversal_amount", "reversal_date",
#             "reversal_settlement_date", "channel", "trans_fee",
#             "merchant_name", "merchant_id", "orig_terminal_id",
#             "fee_in_original_amt", "credit_val", "orig_merchant_name",
#             "text_mess", "agent_acct", "scheme_log_code", "source_bank",
#             "dest_bank", "status_code", "debit_account", "trf_rrn"
#         ]
#
#
# @registry.register_document
# class SettlementDetailDocument(Document):
#     class Index:
#         name = 'settlement_index'
#         settings = {'number_of_shards': 2, 'number_of_replicas': 1}
#
#     class Django:
#         model = SettlementDetail
#
#         fields = [
#             "department_table_id", "department", "debitacct", "debitval", "creditacct", "creditval",
#             "term", "tlocation", "fullrem", "tranamount", "maskedpan", "pan", "trandate", "expdate",
#             "cpd", "settlementdate", "opdate", "transtime", "approval", "issfiid", "acqfiid", "iss_stan",
#             "acqstan", "issuer", "acquirer", "isscountry", "acqcountry", "acquirer_ref", "trancur",
#             "settlementcurrency", "mbr", "mcc", "mcc_description", "stan", "docno", "remark", "msc1_rate",
#             "msc1_cap", "msc2_rate", "msc2_cap", "sharingmsc1_rate", "unsharedmsc1_rate", "subsidy_rate",
#             "msc1_amount", "msc2_amount", "sharingmsc1_amount", "unsharedmsc1_amount", "subsidy_amount",
#             "perc_allotmerchant", "total_amountdue", "deduction_amountdue", "net_amountduemerchant", "irf",
#             "carf", "specialcard", "specialcard_owner", "originalamount", "reasonforalteration",
#             "fullrem_translation", "traceno", "host", "origid", "extrrn", "origtype", "invoicenum",
#             "seqnum", "textmess", "trannumber", "time", "lcyamount", "fcyamount", "merchantid",
#             "merchantname", "merchantdepositbank", "merchantoutletname", "fio", "exchangerate",
#             "settlementaccount", "settlementamount", "bifee", "reversereqid", "sectorcode", "sector",
#             "tranxsaved", "cardcountry", "sign", "processingfee", "customeraccount", "phoneno",
#             "process_status", "cardscheme", "createdate", "meb_issfiid", "acqr_name", "acqr_rate",
#             "acqr_value", "acqr_account", "issr_name", "issr_rate", "issr_value", "issr_account",
#             "ptsp_name", "ptsp_rate", "ptsp_value", "ptsp_account", "ptsa_name", "ptsa_rate",
#             "ptsa_value", "ptsa_account", "terw_name", "terw_rate", "terw_value", "terw_account",
#             "swth_name", "swth_rate", "swth_value", "swth_account", "bilr_name", "bilr_rate",
#             "bilr_value", "bilr_account", "tech_name", "tech_rate", "tech_value", "tech_account",
#             "coaq_name", "coaq_rate", "coaq_value", "coaq_account", "revw_name", "revw_rate",
#             "revw_value", "revw_account", "govt_name", "govt_rate", "govt_value", "govt_account",
#             "agnt_name", "agnt_rate", "agnt_value", "agnt_account", "vend_name", "vend_rate",
#             "vend_value", "vend_account", "recordid", "standard_msc", "standard_cap",
#             "sharingmsc1_diffrate", "sharingmsc1_diffamount", "total_msc", "msc2_splitsharing",
#             "merchantdepositbankcode", "terminstid", "merchantdepositbankacctno", "msc_amount",
#             "effectivemsc", "batchno", "msc2_splitsharingacct", "perc_allotothers", "perc_allotupsl",
#             "userdata", "irf_rate", "amountdue_issuer", "customerid", "old_textmess", "meb_batchno",
#             "opdate2", "ptsp_code", "terw_code", "ptsa_code", "swth_code", "mscconcession_rate",
#             "mscconcession_amount", "serviceprovider", "serviceproviderbankcode", "serviceproviderbank",
#             "serviceprovideraccountno", "beneficiaryname", "beneficiarybankcode", "beneficiarybank",
#             "beneficiaryaccountno", "vend_code", "settlement_service", "effective_sharingamount",
#             "agnt_code", "pros_name", "pros_rate", "pros_value", "pros_account", "blrr_name",
#             "blrr_rate", "blrr_value", "blrr_account", "toacct2", "extpaymentfields", "reportflag",
#             "settlementdate_prev", "reportdate", "cleanupflag", "vast_code", "vast_name", "vast_value",
#             "vast_rate", "vast_account", "pros_code", "bllr_code", "amountorigdcc", "currencyorigdcc",
#             "servicetype", "uphss_transref", "agnt_bank", "agnt_bankcode", "pros_bank", "pros_bankcode",
#             "vast_bank", "vast_bankcode", "acqr_bank", "acqr_bankcode", "issr_bank", "issr_bankcode",
#             "ptsp_bank", "ptsp_bankcode", "ptsa_bank", "ptsa_bankcode", "terw_bank", "terw_bankcode",
#             "proc_bank", "proc_bankcode", "blrr_code", "swth_bank", "swth_bankcode", "fee1", "fee2",
#             "bllr_value", "bllr_bank", "bllr_bankcode", "bllr_account", "bllr_name", "vend_bank",
#             "vend_bankcode", "blrr_bank", "blrr_bankcode", "margin", "revenuecode", "reporttype",
#             "stampdutycharge", "sdsubscalbasis", "sdsubsvalue", "vatcharge", "vatcalbasis", "acqrvat",
#             "issrvat", "terwvat", "swthvat", "ptspvat", "ptsavat", "marginvat", "procvat",
#             "merchantdepositbankcode1", "merchantdepositbank1", "net_amountduemerchant1", "perchantage1",
#             "settlementaccount1", "merchantdepositbankcode2", "merchantdepositbank2",
#             "net_amountduemerchant2", "perchantage2", "settlementaccount2", "merchantdepositbankcode3",
#             "merchantdepositbank3", "net_amountduemerchant3", "perchantage3", "settlementaccount3",
#             "merchantdepositbankcode4", "merchantdepositbank4", "net_amountduemerchant4", "perchantage4",
#             "settlementaccount4", "merchantdepositbankcode5", "merchantdepositbank5",
#             "net_amountduemerchant5", "perchantage5", "settlementaccount5", "superagentrate",
#             "superagentvalue", "superagentacctname", "superagentacct", "uprate", "totalagentfee",
#             "outboundnonbankfee", "qocfee", "merchantdepositbankcode6", "merchantdepositbank6",
#             "net_amountduemerchant6", "perchantage6", "settlementaccount6"
#         ]
#
#
# @registry.register_document
# class BespokeDocument(Document):
#     class Index:
#         name = 'bespoke_index'
#         settings = {'number_of_shards': 2, 'number_of_replicas': 1}
#
#     class Django:
#         model = Bespoke
#
#         fields = [
#         "department_table_id",
#         'transaction_type',
#         'transaction_time',
#         'channel',
#         'aquirer_terminal_id',
#         'acquirer_ps_name',
#         'acquirer_institution_id',
#         'acquirer_country',
#
#         'acquirer_term_state',
#         'acquirer_term_city',
#         'term_owner',
#         'term_entry_caps',
#         'from_account',
#         'to_account',
#         'to_date_field',
#         'from_date',
#         'pan',
#         'bonus',
#         'currency',
#         'error',
#         'proc_duration',
#         'auth_duration',
#         'transaction_number',
#         'issuer_country',
#         'issuer_institution_id',
#         'card_type',
#         'issuer_institution_name',
#         'account_curreny',
#         'curreny_orig',
#         'amount',
#         'transaction_status',
#         'tran_category',
#         'merchant_id',
#         'terminal_retailer',
#         'auth_ps_name',
#         'pos_entry_mode',
#         'department',
#         'beneficiary_bank',
#         'beneficiary_name',
#         'rrn',
#         'status',
#         'status_code',
#         'description',
#         'stan',
#         'fee',
#         'total_amount',
#         'merchant_fee',
#         'merchant_account',
#         'direction',
#         'authorization_ref',
#         'completed',
#         'narration',
#         'beneficiary',
#         'sender_name',
#         'sender_bank',
#         'sender_account',
#         'is_offline',
#         'issuer',
#         'acquirer',
#         'location_field',
#         'reversal_transaction_id',
#         'request_id',
#         'actual_date',
#         'reversal_date',
#         'is_reversal',
#         'reversed_transaction_id',
#         'reversal_reason',
#         'proc_code',
#         'product_code',
#         'accesslog_id',
#         'mcc',
#         'terminal_date',
#         'agent',
#         'balance',
#         'ip',
#         'beneficiary_uan',
#         'reference_trx_id',
#         'holder_id',
#         'is_payment_request',
#         'receipt',
#         'depositor',
#         'super_agent_commission',
#         'sub_agent_commission',
#         'agent_id',
#         'agent_code',
#         'super_agent_code',
#         'debit_code',
#         'credit_route',
#         'issuer_fee',
#         'debit_route',
#         'ussd_fee',
#         'ussd_network',
#         ]
