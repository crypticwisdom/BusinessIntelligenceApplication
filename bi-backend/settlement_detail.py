# ========= sample ==========
# id SERIAL PRIMARY KEY,
# float_column_1 REAL,
# float_column_2 FLOAT, # in use
# integer_column INTEGER,
# text_column TEXT,
# varchar_column VARCHAR(255)
# ========= sample ==========
#
# SCHEMA: etl_schema
# # ============== process_record ================= =
# CREATE TABLE etl_schema.process_record (
#     id SERIAL PRIMARY KEY,
#     script_name TEXT,
#     record INTEGER,
#     last_date_ran TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# );
#
# -- done --
# ================ PAYARENA EXCHANGE ===============
# CREATE TABLE etl_schema.payarena_exchange (
#     id SERIAL PRIMARY KEY,
#     department_table_id VARCHAR(255),
#     department VARCHAR(255),
#     schema_or_table_name VARCHAR(255),
#     request_time TIMESTAMP,
#     response_time TIMESTAMP,
#     account_no VARCHAR(255),
#     amount VARCHAR(255),
#     request_id VARCHAR(255),
#     request_type VARCHAR(255),
#     route VARCHAR(255),
#     status_code VARCHAR(255),
#     status_message VARCHAR(255),
#     response_id VARCHAR(255),
#     rrn VARCHAR(255),
#     client VARCHAR(255),
#     reversed VARCHAR(255),
#     reversible VARCHAR(255),
#     batch_id VARCHAR(255),
#     beneficiary VARCHAR(255),
#     sender VARCHAR(255),
#     reference VARCHAR(255),
#     channel VARCHAR(255),
#     source_bank VARCHAR(255),
#     txid VARCHAR(255),
#     dest_bank VARCHAR(255)
# );
#
#
# ================ BANK BRANCH ===================
# CREATE TABLE etl_schema.bank_branch_etl (
#     id SERIAL PRIMARY KEY,
#     department_table_id VARCHAR(255),
#     department VARCHAR(255),
#     schema_or_table_name VARCHAR(255),
#     op_date TIMESTAMP,
#     gid VARCHAR(255),
#     item_no VARCHAR(255),
#     tran_no VARCHAR(255),
#     applicant_name VARCHAR(255),
#     app_type VARCHAR(255),
#     tran_amount VARCHAR(255),
#     app_id VARCHAR(255),
#     tran_date TIMESTAMP,
#     full_rem VARCHAR(255),
#     payment_mode VARCHAR(255),
#     response_code VARCHAR(255),
#     state_code VARCHAR(255),
#     state_name VARCHAR(255),
#     approval_code VARCHAR(255),
#     bank_name VARCHAR(255),
#     bank_code VARCHAR(255),
#     account_name_bank VARCHAR(255),
#     retailer_id VARCHAR(255),
#     term_id VARCHAR(255),
#     tran_currency VARCHAR(255),
#     branch_code VARCHAR(255),
#     payment_status VARCHAR(255),
#     reference_no VARCHAR(255),
#     settled_status VARCHAR(255),
#     deposit_slip_no VARCHAR(255),
#     response VARCHAR(255)
# );
#
# ================== NEW CARD ACCOUNT DETAIL ISSUING ======================
# CREATE TABLE etl_schema.card_account_details_issuing (
#     id SERIAL PRIMARY KEY,
#     department_table_id VARCHAR(255),
#     department VARCHAR(255),
#     schema_or_table_name VARCHAR(255),
#     client_id VARCHAR(255),
#     branch FLOAT,
#     pan VARCHAR(255),
#     account_no VARCHAR(255),
#     card_type VARCHAR(255),
#     tcard_createdate TIMESTAMP,
#     tcard_updatedate TIMESTAMP,
#     tcard_crd_stat VARCHAR(255),
#     tcard_sign_stat INTEGER,
#     taccount_stat VARCHAR(255),
#     tcard_orderdate TIMESTAMP,
#     tcard_canceldate TIMESTAMP,
#     tcard_makingdate TIMESTAMP,
#     tcard_distributiondate TIMESTAMP,
#     taccount_closedate TIMESTAMP,
#     taccount_updatedate TIMESTAMP,
#     currency_no VARCHAR(255),
#     insure VARCHAR(255),
#     cvv VARCHAR(255),
#     cvv2 VARCHAR(255),
#     making_priority VARCHAR(255),
#     parent_mbr VARCHAR(255),
#     grp_limit VARCHAR(255),
#     pin_block VARCHAR(255),
#     card_product VARCHAR(255),
#     ipvv VARCHAR(255),
#     name_on_card VARCHAR(255),
#     create_sys_date TIMESTAMP,
#     close_sys_date TIMESTAMP,
#     risk_level VARCHAR(255),
#     remake VARCHAR(255),
#     remark VARCHAR(255),
#     guid VARCHAR(255),
#     contactlesscapable VARCHAR(255),
#     protected_amount VARCHAR(255),
#     existsinpc VARCHAR(255),
#     counter_grpid VARCHAR(255),
#     internal_limits_group_sys_id VARCHAR(255),
#     remain_update_date TIMESTAMP,
#     update_revision VARCHAR(255),
#     limit_grp_id VARCHAR(255),
#     lower_main VARCHAR(255),
#     credit_reserve VARCHAR(255),
#     debit_reserve VARCHAR(255),
#     acct_stat VARCHAR(255),
#     acct_typ VARCHAR(255),
#     update_sys_date TIMESTAMP,
#     update_clerk_code VARCHAR(255),
#     create_clerk_code VARCHAR(255),
#     over_draft VARCHAR(255),
#     available VARCHAR(255),
#     account_type VARCHAR(255),
#     taccount_createdate TIMESTAMP
# );
#
#
# =============== CARD CLIENT PERSONNEL ================
# CREATE TABLE etl_schema.card_client_personnel_issuing (
#     id SERIAL PRIMARY KEY,
#     department_table_id VARCHAR(255),
#     department VARCHAR(255),
#     branch VARCHAR(255),
#     client_id VARCHAR(255),
#     country_res VARCHAR(255),
#     sex VARCHAR(255),
#     birthday TIMESTAMP,
#     birth_place VARCHAR(255),
#     foreign_pass VARCHAR(255),
#     foreign_exp TIMESTAMP,
#     country_reg VARCHAR(255),
#     region_reg VARCHAR(255),
#     city_reg VARCHAR(255),
#     zip_reg VARCHAR(255),
#     address_reg VARCHAR(255),
#     country_live VARCHAR(255),
#     region_live VARCHAR(255),
#     city_live VARCHAR(255),
#     zip_live VARCHAR(255),
#     address VARCHAR(255),
#     country_cont VARCHAR(255),
#     region_cont VARCHAR(255),
#     city_cont VARCHAR(255),
#     zip_cont VARCHAR(255),
#     address_cont VARCHAR(255),
#     phone VARCHAR(255),
#     fax VARCHAR(255),
#     email VARCHAR(255),
#     mobile_phone VARCHAR(255),
#     pager VARCHAR(255),
#     company VARCHAR(255),
#     office VARCHAR(255),
#     tab_no VARCHAR(255),
#     job_title VARCHAR(255),
#     job_phone VARCHAR(255),
#     date_create TIMESTAMP,
#     date_modify TIMESTAMP,
#     occupation VARCHAR(255),
#     vip VARCHAR(255),
#     insider VARCHAR(255),
#     resident VARCHAR(255),
#     title VARCHAR(255),
#     street_reg VARCHAR(255),
#     house_reg VARCHAR(255),
#     death_date TIMESTAMP,
#     first_name VARCHAR(255),
#     middle_name VARCHAR(255),
#     last_name VARCHAR(255),
#     category_id VARCHAR(255),
#     grp_limit VARCHAR(255),
#     grp_limit_int VARCHAR(255),
#     limit_currency VARCHAR(255),
#     schema_or_table_name VARCHAR(255),
#     bankrupt VARCHAR(255)
# );
#
#
# ============== etl_schema.cipa_parties_merchant ================
# CREATE TABLE etl_schema.cipa_parties_merchant (
#     id SERIAL PRIMARY KEY,
#     code VARCHAR(255),
#     name VARCHAR(255),
#     bank VARCHAR(255),
#     account VARCHAR(255),
#     email VARCHAR(255),
#     isenabled VARCHAR(255),
#     ismerchant VARCHAR(255),
#     url VARCHAR(255),
#     profileid VARCHAR(255),
#     feeincluded VARCHAR(255),
#     secretkey VARCHAR(255),
#     ipaddresses VARCHAR(255),
#     merchantid VARCHAR(255),
#     merid VARCHAR(255),
#     profilename VARCHAR(255),
#     terminalid VARCHAR(255),
#     discriminator VARCHAR(255),
#     retailerid VARCHAR(255),
#     migrated VARCHAR(255),
#     amexid VARCHAR(255),
#     iscardenabled VARCHAR(255),
#     isrecurring VARCHAR(255),
#     payattitudefee VARCHAR(255),
#     cardfee VARCHAR(255),
#     maxfee VARCHAR(255),
#     vat VARCHAR(255),
#     addvat VARCHAR(255),
#     encryptionkey VARCHAR(255),
#     encryptioniv VARCHAR(255),
#     reversalenabled VARCHAR(255),
#     enablecardonfile VARCHAR(255),
#     cardonfileref VARCHAR(255),
#     cardonfileid VARCHAR(255),
#     ordertimeout VARCHAR(255),
#     disablepayattitude VARCHAR(255)
# );
#
# -- done --
# ========= holdertags =========
# CREATE TABLE etl_schema.holdertags (
#     id SERIAL PRIMARY KEY,
#     department TEXT,
#     schema_type TEXT,
#     phone TEXT,
#     holderid TEXT,
#     isactive TEXT,
#     isprepaid TEXT,
#     prepaidaccountid TEXT,
#     isdebit TEXT,
#     bankaccount TEXT,
#     pin TEXT,
#     pinstreak TEXT,
#     status TEXT,
#     created TEXT,
#     branch TEXT,
#     onlineprofile TEXT,
#     offlineprofile TEXT,
#     institution TEXT,
#     agentid TEXT,
#     ispremium TEXT,
#     recurringsubscriptionfee TEXT,
#     tagstatus TEXT,
#     activationdate TEXT,
#     lastpaymentdate TEXT,
#     nextpaymentdate TEXT,
#     subtype TEXT,
#     subscriptiontransactionid TEXT,
#     activationtransactionid TEXT,
#     nickname TEXT,
#     isverified1 TEXT,
#     verificationaccesstime TEXT,
#     verificationcomment TEXT,
#     selectcount TEXT,
#     verifier TEXT,
#     isverified TEXT,
#     agentcode TEXT,
#     pendforadditionalaccount TEXT,
#     isdefault TEXT,
#     channel TEXT,
#     recomputed TEXT,
#     email TEXT,
#     firstname TEXT,
#     lastname TEXT,
#     middlename TEXT,
#     address TEXT,
#     city TEXT,
#     state TEXT,
#     nationality TEXT,
#     sex TEXT,
#     marriage TEXT,
#     profession TEXT,
#     occupation TEXT,
#     bvn TEXT,
#     birthday TEXT,
#     premiumactivated TEXT,
#     accountid TEXT,
#     isagent TEXT,
#     isverified2 TEXT,
#     nin TEXT
# );
#
#
# ========= transaction_type =========
# CREATE TABLE etl_schema.transaction_type (
#     id SERIAL PRIMARY KEY,
#     bespoke_name TEXT,
#     bespoke_code TEXT,
#     processing_code BIGINT,
#     processing_name TEXT,
#     processing_constant_name TEXT,
#     processing_class TEXT
# );
#
# -- done --
# issuer - request_id
# =========== transaction =============
# CREATE TABLE etl_schema.transactions (
#     id SERIAL PRIMARY KEY,
#     department_table_id VARCHAR(255),
#     department VARCHAR(255),
#     transaction_type VARCHAR(255),
#     transaction_time TIMESTAMP,
#     original_time TIMESTAMP,
#     channel VARCHAR(255),
#     secondary_channel VARCHAR(255),
#     aquirer_terminal_id VARCHAR(1255),
#     acquirer_ps_name VARCHAR(1255),
#     acquirer_institution_id VARCHAR(1255),
#     beneficiary_bank VARCHAR(1255),
#     beneficiary_name VARCHAR(1255),
#     acquirer_country VARCHAR(1255),
#     acquirer_term_state VARCHAR(1255),
#     acquirer_term_city VARCHAR(1255),
#     term_owner VARCHAR(1255),
#     term_entry_caps VARCHAR(1255),
#     from_account VARCHAR(1255),
#     to_account VARCHAR(1255),
#     to_date_field TIMESTAMP,
#     from_date TIMESTAMP,
#     pan VARCHAR(1255),
#     bonus VARCHAR(1255),
#     currency VARCHAR(1255),
#     error VARCHAR(1255),
#     proc_duration VARCHAR(1255),
#     account_id VARCHAR(1255),
#     phone VARCHAR(1255),
#     bank VARCHAR(1255),
#     trx_type VARCHAR(1255),
#     terminal_id VARCHAR(1255),
#     tag VARCHAR(1255),
#     auth_duration VARCHAR(1255),
#     transaction_number VARCHAR(1255),
#     rrn VARCHAR(1255),
#     issuer_country VARCHAR(1255),
#     issuer_institution_id VARCHAR(1255),
#     card_type VARCHAR(1255),
#     issuer_institution_name VARCHAR(1255),
#     account_curreny VARCHAR(1255),
#     curreny_orig VARCHAR(1255),
#     amount FLOAT,
#     transaction_status VARCHAR(1255),
#     status VARCHAR(1255),
#     status_code VARCHAR(1255),
#     tran_category VARCHAR(1255),
#     merchant_id VARCHAR(1255),
#     terminal_retailer VARCHAR(1255),
#     auth_ps_name VARCHAR(1255),
#     pos_entry_mode VARCHAR(1255),
#     description VARCHAR(1255),
#     stan VARCHAR(1255),
#     fee FLOAT,
#     total_amount FLOAT,
#     merchant_fee FLOAT,
#     merchant_account VARCHAR(1255),
#     direction VARCHAR(1255),
#     authorization_ref VARCHAR(1255),
#     completed VARCHAR(1255),
#     narration VARCHAR(1255),
#     beneficiary VARCHAR(1255),
#     sender_name VARCHAR(1255),
#     sender_bank VARCHAR(1255),
#     sender_account VARCHAR(1255),
#     is_offline VARCHAR(1255),
#     issuer VARCHAR(1255),
#     acquirer VARCHAR(1255),
#     location_field VARCHAR(1255),
#     reversal_transaction_id VARCHAR(1255),
#     request_id VARCHAR(1255),
#     actual_date TIMESTAMP,
#     reversal_date TIMESTAMP,
#     is_reversal VARCHAR(1255),
#     reversed_transaction_id VARCHAR(1255),
#     reversal_reason VARCHAR(1255),
#     proc_code VARCHAR(1255),
#     product_code VARCHAR(1255),
#     accesslog_id VARCHAR(1255),
#     mcc VARCHAR(1255),
#     terminal_date TIMESTAMP,
#     agent VARCHAR(1255),
#     balance VARCHAR(1255),
#     ip VARCHAR(1255),
#     beneficiary_uan VARCHAR(1255),
#     reference_trx_id VARCHAR(1255),
#     holder_id VARCHAR(1255),
#     is_payment_request VARCHAR(1255),
#     receipt VARCHAR(1255),
#     depositor VARCHAR(1255),
#     super_agent_commission VARCHAR(1255),
#     sub_agent_commission VARCHAR(1255),
#     agent_id VARCHAR(1255),
#     agent_code VARCHAR(1255),
#     super_agent_code VARCHAR(1255),
#     debit_code VARCHAR(1255),
#     credit_code VARCHAR(1255),
#     credit_route VARCHAR(1255),
#     issuer_fee FLOAT,
#     debit_route VARCHAR(1255),
#     ussd_fee FLOAT,
#     ussd_network VARCHAR(1255)
# );
#
#
# -- done --
# ============= up_con_terminal_config =============
# CREATE TABLE etl_schema.up_con_terminal_config (
#     id SERIAL PRIMARY KEY,
#     department_table_id VARCHAR(255),
#     department VARCHAR(255),
#     schema_or_table_name VARCHAR(255),
#     tid VARCHAR(255),
#     mid VARCHAR(255),
#     serial_number VARCHAR(255),
#     contact_title VARCHAR(255),
#     contact_name VARCHAR(255),
#     init_application_version VARCHAR(255),
#     merchant_name VARCHAR(255),
#     profile_name VARCHAR(255),
#     merchant_address VARCHAR(255),
#     contact_phone VARCHAR(255),
#     email VARCHAR(255),
#     state_code VARCHAR(255),
#     date_created VARCHAR(255),
#     bank_accno VARCHAR(255),
#     lga VARCHAR(255),
#     app_name VARCHAR(255),
#     terminal_owner_code VARCHAR(255),
#     terminal_manufacturer VARCHAR(255),
#     terminal_model VARCHAR(255),
#     added_by VARCHAR(255),
#     if_available VARCHAR(255),
#     tid_pin VARCHAR(255),
#     physical_address VARCHAR(255),
#     merchant_cat_code VARCHAR(255),
#     ptsp VARCHAR(255),
#     mechant_account_name VARCHAR(255),
#     bank_code VARCHAR(255),
#     name_created VARCHAR(255),
#     date_modified VARCHAR(255),
#     name_modified VARCHAR(255),
#     email_alerts VARCHAR(255),
#     visa_acquirer_id_number VARCHAR(255),
#     master_card_acquirer_id_number VARCHAR(255),
#     verve_acquirer_id_number VARCHAR(255),
#     account_name VARCHAR(255),
#     terminal_type VARCHAR(255),
#     country VARCHAR(255),
#     country_code VARCHAR(255),
#     payattitude_acquirer_id_number VARCHAR(255),
#     cup_acquirer_id_number VARCHAR(255),
#     jcb_acquirer_id_number VARCHAR(255),
#     maker VARCHAR(255),
#     checker VARCHAR(255),
#     bank_name VARCHAR(255),
#     term_ps_name VARCHAR(255),
#     payarenam_id VARCHAR(255),
#     payarena_tid VARCHAR(255),
#     comm_sid VARCHAR(255),
#     timestamp_field TIMESTAMP,
#     terminal_model_code VARCHAR(255),
#     pan VARCHAR(255),
#     proc_code VARCHAR(255),
#     rrn VARCHAR(255),
#     stan VARCHAR(255),
#     message_date TIMESTAMP,
#     message_started_date TIMESTAMP,
#     resp_code VARCHAR(255),
#     auth_code VARCHAR(255),
#     message_date_completed TIMESTAMP,
#     amount FLOAT,
#     currency VARCHAR(255),
#     source VARCHAR(255),
#     destination VARCHAR(255),
#     hostrespcode VARCHAR(255)
# );
#
# -- done --
# ============ up_dms ==============
# CREATE TABLE etl_schema.up_dms (
#     id SERIAL PRIMARY KEY,
#     trans_date TIMESTAMP,
#     settlement_date TIMESTAMP,
#     dispute_date TIMESTAMP,
#     due_date TIMESTAMP,
#     reversal_date TIMESTAMP,
#     reversal_settlement_date TIMESTAMP,
#     cpd TIMESTAMP,
#     dispute_status VARCHAR(255),
#     location VARCHAR(255),
#     trans_amount VARCHAR(255),
#     trans_time VARCHAR(255),
#     masked_pan VARCHAR(255),
#     acct_no VARCHAR(255),
#     trans_type VARCHAR(255),
#     response VARCHAR(255),
#     issuer VARCHAR(255),
#     acquirer VARCHAR(255),
#     issuer_stan VARCHAR(255),
#     acquirer_stan VARCHAR(255),
#     acquirer_rrn VARCHAR(255),
#     issuer_rrn VARCHAR(255),
#     order_id VARCHAR(255),
#     dispense_amount VARCHAR(255),
#     log_comment VARCHAR(255),
#     full_access VARCHAR(255),
#     reversal_id VARCHAR(255),
#     reversal_amount VARCHAR(255),
#     channel VARCHAR(255),
#     trans_fee VARCHAR(255),
#     merchant_name VARCHAR(255),
#     merchant_id VARCHAR(255),
#     orig_terminal_id VARCHAR(255),
#     fee_in_original_amt VARCHAR(255),
#     credit_val VARCHAR(255),
#     orig_merchant_name VARCHAR(255),
#     text_mess VARCHAR(255),
#     agent_acct VARCHAR(255),
#     scheme_log_code VARCHAR(255),
#     source_bank VARCHAR(255),
#     dest_bank VARCHAR(255),
#     status_code VARCHAR(255),
#     debit_account VARCHAR(255),
#     trf_rrn VARCHAR(255),
#     department_table_id VARCHAR(255),
#     department VARCHAR(255),
#     schema_or_table_name VARCHAR(255),
#     terminal_id VARCHAR(255),
#     trans_id VARCHAR(255),
#     dispute_id VARCHAR(255),
#     approval_code VARCHAR(255)
# );
#
# --- done ---
# # ============ settlement_meb ============
# CREATE TABLE etl_schema.settlement_meb (
#     id SERIAL PRIMARY KEY,
#     department_table_id VARCHAR(1255),
#     department VARCHAR(1255),
#     opdate TIMESTAMP,
#     clerkcode VARCHAR(1255),
#     station VARCHAR(1255),
#     docno VARCHAR(1255),
#     no VARCHAR(1255),
#     entcode VARCHAR(1255),
#     debitacct VARCHAR(1255),
#     debitval VARCHAR(1255),
#     creditacct VARCHAR(1255),
#     creditval VARCHAR(1255),
#     shortrem VARCHAR(1255),
#     fullrem VARCHAR(1255),
#     device VARCHAR(1255),
#     extentcode VARCHAR(1255),
#     trandate VARCHAR(1255),
#     issficode VARCHAR(1255),
#     issfiid VARCHAR(1255),
#     issps VARCHAR(1225),
#     isscountry VARCHAR(1255),
#     pan VARCHAR(1255),
#     mbr VARCHAR(1255),
#     expdate TIMESTAMP,
#     approval VARCHAR(1255),
#     acqficode VARCHAR(1255),
#     acqfiid VARCHAR(1255),
#     acqps VARCHAR(1255),
#     acqcountry VARCHAR(1255),
#     termcode VARCHAR(1255),
#     term VARCHAR(1255),
#     retailer VARCHAR(1255),
#     tlocation VARCHAR(1255),
#     mcc VARCHAR(1255),
#     stan VARCHAR(1255),
#     acqiin VARCHAR(1255),
#     forwardiin VARCHAR(1255),
#     invoice VARCHAR(1255),
#     userdata VARCHAR(1255),
#     tranamount VARCHAR(1255),
#     trancur VARCHAR(1255),
#     orgamount VARCHAR(1255),
#     orgcur VARCHAR(1255),
#     acqfee VARCHAR(1255),
#     acqfeecur VARCHAR(1255),
#     issfee VARCHAR(1255),
#     issfeecur VARCHAR(1255),
#     intrfee VARCHAR(1255),
#     intrfeecur VARCHAR(1255),
#     ident1 VARCHAR(1255),
#     info1 VARCHAR(1255),
#     ident2 VARCHAR(1255),
#     info2 VARCHAR(1255),
#     ident3 VARCHAR(1255),
#     info3 VARCHAR(1255),
#     debitcur VARCHAR(1255),
#     creditcur VARCHAR(1255),
#     crnum VARCHAR(1255),
#     bifee VARCHAR(1255),
#     usdfn VARCHAR(1255),
#     usdfn2 VARCHAR(1255),
#     usdfn3 VARCHAR(1255),
#     process_status VARCHAR(1255),
#     remark VARCHAR(1255),
#     recordid VARCHAR(1255),
#     transign VARCHAR(1255),
#     batchno VARCHAR(1255),
#     code VARCHAR(1255),
#     opdate2 TIMESTAMP,
#     createdate TIMESTAMP,
#     info1_temp VARCHAR(1255),
#     scheme VARCHAR(1255),
#     DOCNO_TEMP VARCHAR(1255),
#     GENERATEDFLAG VARCHAR(1255),
#     PACKNO VARCHAR(1255),
#     TRANNUMBER VARCHAR(1255),
#     TYPE VARCHAR(1255),
#     ORIGTYPE VARCHAR(1255),
#     ORIGID VARCHAR(1255),
#     ORIGUNIT VARCHAR(1255),
#     HOST VARCHAR(1255),
#     HOSTINTERFACE VARCHAR(1255),
#     TIME TIMESTAMP,
#     ORIGTIME TIMESTAMP,
#     PHASE VARCHAR(1255),
#     TERMCLASS VARCHAR(1255),
#     TERMNAME VARCHAR(1255),
#     TERMNAME2 VARCHAR(1255),
#     TERMDATE TIMESTAMP,
#     TERMPSNAME VARCHAR(1255),
#     TERMFIID VARCHAR(1255),
#     TERMFINAME VARCHAR(1255),
#     TERMINSTID VARCHAR(1255),
#     TERMRETAILER VARCHAR(1255),
#     TERMRETAILERNAME VARCHAR(1255),
#     TERMSIC VARCHAR(1255),
#     TERMINSTCOUNTRY VARCHAR(1255),
#     TERMCOUNTRY VARCHAR(1255),
#     TERMCOUNTY VARCHAR(1255),
#     TERMSTATE VARCHAR(1255),
#     TERMREGION VARCHAR(1255),
#     TERMCITY VARCHAR(1255),
#     TERMZIP VARCHAR(1255),
#     TERMLOCATION VARCHAR(1255),
#     TERMDESCRIPTION VARCHAR(1255),
#     TERMBRANCH VARCHAR(1255),
#     TERMOWNER VARCHAR(1255),
#     TERMTIMEOFFSET VARCHAR(1255),
#     TERMAPPRVCODELEN VARCHAR(1255),
#     TERMENTRYCAPS VARCHAR(1255),
#     AUTHFIID VARCHAR(1255),
#     AUTHFINAME VARCHAR(1255),
#     AUTHPSNAME VARCHAR(1255),
#     SPONSORFIID VARCHAR(1255),
#     TRANCODE VARCHAR(1255),
#     TRANCATEGORY VARCHAR(1255),
#     DRAFTCAPTURE VARCHAR(1255),
#     FROMACCTTYPE VARCHAR(1255),
#     FROMACCT VARCHAR(1255),
#     FROMACCTDESCR VARCHAR(1255),
#     TOACCTTYPE VARCHAR(1255),
#     TOACCT VARCHAR(1255),
#     TOACCTDESCR VARCHAR(1255),
#     TOACCT2 VARCHAR(1255),
#     CORRESPACCT VARCHAR(1255),
#     CORRESPAMOUNT VARCHAR(1255),
#     FROMDATE TIMESTAMP,
#     TODATE TIMESTAMP,
#     AMOUNT VARCHAR(1255),
#     AMOUNT2 VARCHAR(1255),
#     FEE VARCHAR(1255),
#     ISSUERFEE VARCHAR(1255),
#     CURRENCY VARCHAR(1255),
#     MESSAGE VARCHAR(1255),
#     CARDMEMBER VARCHAR(1255),
#     PAN2 VARCHAR(1255),
#     CARDMEMBER2 VARCHAR(1255),
#     AUTHPAN VARCHAR(1255),
#     AUTHMBR VARCHAR(1255),
#     PREFIX VARCHAR(1255),
#     CARDPROFILE VARCHAR(1255),
#     ACQGROUP VARCHAR(1255),
#     PIN VARCHAR(1255),
#     INVOICENUM VARCHAR(1255),
#     ORIGINALINVOICENUM VARCHAR(1255),
#     SEQNUM VARCHAR(1255),
#     ORIGINALSEQNUM VARCHAR(1255),
#     POSCONDITION VARCHAR(1255),
#     RESPCODE VARCHAR(1255),
#     DECLINEREASON VARCHAR(1255),
#     RESPONSECONDITION VARCHAR(1255),
#     APPROVALCODE VARCHAR(1255),
#     TEXTMESS VARCHAR(1255),
#     EXTRRN VARCHAR(1255),
#     EXTSTAN VARCHAR(1255),
#     ISSUERCOUNTRY VARCHAR(1255),
#     IRF VARCHAR(1255),
#     INCSTAN VARCHAR(1255),
#     FEP_STAN VARCHAR(1255),
#     EXTPAYMENTFIELDS VARCHAR(4000),
#     REVENUECODE VARCHAR(1255),
#     REVREQUESTID VARCHAR(1255),
#     PREPROCESSSTATUS VARCHAR(1255),
#     POSENTRYMODE VARCHAR(1255)
# );
#
#
# CREATE TABLE etl_schema.settlement_detail (
#     id_ SERIAL PRIMARY KEY,
#     department_table_id VARCHAR,
#     department VARCHAR,
#     PTSA_BANK VARCHAR,
#     UNSHAREDMSC1_RATE NUMERIC,
#     PTSA_CODE VARCHAR,
#     PROS_ACCOUNT VARCHAR,
#     TERW_VALUE NUMERIC,
#     SHARINGMSC1_DIFFRATE NUMERIC,
#     MERCHANTDEPOSITBANKCODE2 VARCHAR,
#     PROCESS_STATUS VARCHAR,
#     MERCHANTDEPOSITBANKCODE VARCHAR,
#     REMARK VARCHAR,
#     PTSA_ACCOUNT VARCHAR,
#     CPD DATE,
#     SERVICEPROVIDER VARCHAR,
#     SWTH_NAME VARCHAR,
#     ACQRVAT NUMERIC,
#     AGNT_CODE VARCHAR,
#     SWTH_BANKCODE VARCHAR,
#     BILR_NAME VARCHAR,
#     BILR_RATE NUMERIC,
#     TERW_BANKCODE VARCHAR,
#     EXCHANGERATE NUMERIC,
#     REVW_VALUE NUMERIC,
#     UPHSS_TRANSREF VARCHAR,
#     SETTLEMENTACCOUNT3 VARCHAR,
#     SUPERAGENTRATE NUMERIC,
#     AGNT_BANKCODE VARCHAR,
#     SWTH_ACCOUNT VARCHAR,
#     MSC2_AMOUNT NUMERIC,
#     OUTBOUNDNONBANKFEE NUMERIC,
#     VEND_CODE VARCHAR,
#     SIGN VARCHAR,
#     SHARINGMSC1_AMOUNT NUMERIC,
#     USERDATA VARCHAR,
#     VAST_BANK VARCHAR,
#     CUSTOMERID NUMERIC,
#     MERCHANTDEPOSITBANK4 VARCHAR,
#     BENEFICIARYBANK VARCHAR,
#     BLRR_VALUE NUMERIC,
#     FULLREM_TRANSLATION VARCHAR,
#     PROC_BANK VARCHAR,
#     PHONENO VARCHAR,
#     TRANSTIME VARCHAR,
#     PTSP_VALUE NUMERIC,
#     VEND_BANKCODE VARCHAR,
#     ACQR_NAME VARCHAR,
#     PROS_VALUE NUMERIC,
#     REVENUECODE VARCHAR,
#     HOST NUMERIC,
#     STANDARD_CAP NUMERIC,
#     SPECIALCARD_OWNER VARCHAR,
#     TERW_RATE NUMERIC,
#     TOTAL_MSC NUMERIC,
#     REVW_RATE NUMERIC,
#     MERCHANTDEPOSITBANK2 VARCHAR,
#     SDSUBSCALBASIS VARCHAR,
#     ISSR_RATE NUMERIC,
#     VEND_ACCOUNT VARCHAR,
#     EFFECTIVE_SHARINGAMOUNT NUMERIC,
#     BLRR_ACCOUNT VARCHAR,
#     NET_AMOUNTDUEMERCHANT5 NUMERIC,
#     UPRATE NUMERIC,
#     GOVT_RATE NUMERIC,
#     MBR NUMERIC,
#     MSC1_RATE NUMERIC,
#     MERCHANTDEPOSITBANKCODE5 VARCHAR,
#     VAST_NAME VARCHAR,
#     STAMPDUTYCHARGE NUMERIC,
#     MERCHANTDEPOSITBANKCODE4 VARCHAR,
#     BLLR_NAME VARCHAR,
#     SETTLEMENTACCOUNT VARCHAR,
#     COAQ_RATE NUMERIC,
#     STANDARD_MSC NUMERIC,
#     PROS_BANKCODE VARCHAR,
#     TECH_VALUE NUMERIC,
#     VATCALBASIS VARCHAR,
#     PROS_CODE VARCHAR,
#     AGNT_VALUE NUMERIC,
#     MERCHANTNAME VARCHAR,
#     ISSR_BANK VARCHAR,
#     SECTORCODE VARCHAR,
#     VEND_RATE NUMERIC,
#     DEBITVAL NUMERIC,
#     SPECIALCARD VARCHAR,
#     ORIGID VARCHAR,
#     VATCHARGE NUMERIC,
#     ISSR_NAME VARCHAR,
#     PERCHANTAGE4 NUMERIC,
#     ISS_STAN VARCHAR,
#     SETTLEMENTCURRENCY NUMERIC,
#     INVOICENUM VARCHAR,
#     SETTLEMENTACCOUNT4 VARCHAR,
#     BLRR_NAME VARCHAR,
#     PROC_BANKCODE VARCHAR,
#     SWTH_VALUE NUMERIC,
#     EFFECTIVEMSC NUMERIC,
#     BILR_VALUE NUMERIC,
#     AGNT_RATE NUMERIC,
#     AMOUNTORIGDCC NUMERIC,
#     BLRR_CODE VARCHAR,
#     BILR_ACCOUNT VARCHAR,
#     MSC_AMOUNT NUMERIC,
#     TRANXSAVED VARCHAR,
#     CARDCOUNTRY VARCHAR,
#     TLOCATION VARCHAR,
#     PTSP_ACCOUNT VARCHAR,
#     MERCHANTDEPOSITBANK6 VARCHAR,
#     ACQUIRER VARCHAR,
#     PROS_NAME VARCHAR,
#     REPORTDATE DATE,
#     REPORTTYPE VARCHAR,
#     PTSA_NAME VARCHAR,
#     PERC_ALLOTMERCHANT NUMERIC,
#     PERCHANTAGE3 NUMERIC,
#     PERCHANTAGE5 NUMERIC,
#     TERM VARCHAR,
#     ACQSTAN VARCHAR,
#     APPROVAL VARCHAR,
#     ISSUER VARCHAR,
#     TERWVAT NUMERIC,
#     BENEFICIARYACCOUNTNO VARCHAR,
#     VEND_BANK VARCHAR,
#     ISSCOUNTRY NUMERIC,
#     PERCHANTAGE6 NUMERIC,
#     EXTRRN VARCHAR,
#     SWTHVAT NUMERIC,
#     SERVICETYPE NUMERIC,
#     PTSP_BANK VARCHAR,
#     SHARINGMSC1_RATE NUMERIC,
#     PTSPVAT NUMERIC,
#     QOCFEE NUMERIC,
#     BLLR_BANKCODE VARCHAR,
#     ISSRVAT NUMERIC,
#     PROCVAT NUMERIC,
#     MSC2_SPLITSHARING VARCHAR,
#     SUPERAGENTACCT VARCHAR,
#     FCYAMOUNT NUMERIC,
#     SHARINGMSC1_DIFFAMOUNT NUMERIC,
#     TERMINSTID VARCHAR,
#     BATCHNO NUMERIC,
#     PTSP_CODE VARCHAR,
#     RECORDID NUMERIC,
#     MSC1_CAP NUMERIC,
#     MERCHANTDEPOSITBANK1 VARCHAR,
#     COAQ_NAME VARCHAR,
#     SEQNUM VARCHAR,
#     CURRENCYORIGDCC NUMERIC,
#     ACQR_RATE NUMERIC,
#     FEE2 NUMERIC,
#     SDSUBSVALUE NUMERIC,
#     GOVT_NAME VARCHAR,
#     DOCNO VARCHAR,
#     SUBSIDY_RATE NUMERIC,
#     BLRR_RATE NUMERIC,
#     VAST_ACCOUNT VARCHAR,
#     TRANAMOUNT NUMERIC,
#     OPDATE DATE,
#     PROCESSINGFEE NUMERIC,
#     ISSR_BANKCODE VARCHAR,
#     BIFEE VARCHAR,
#     CREATEDATE DATE,
#     CREDITVAL NUMERIC,
#     SERVICEPROVIDERACCOUNTNO VARCHAR,
#     BLLR_VALUE NUMERIC,
#     TIME DATE,
#     MSC1_AMOUNT NUMERIC,
#     NET_AMOUNTDUEMERCHANT2 NUMERIC,
#     SECTOR VARCHAR,
#     GOVT_ACCOUNT VARCHAR,
#     MSC2_CAP NUMERIC,
#     COAQ_VALUE NUMERIC,
#     PAN VARCHAR,
#     MERCHANTDEPOSITBANK VARCHAR,
#     MEB_BATCHNO VARCHAR,
#     MSCCONCESSION_AMOUNT NUMERIC,
#     MCC VARCHAR,
#     IRF NUMERIC,
#     SWTH_CODE VARCHAR,
#     MERCHANTOUTLETNAME VARCHAR,
#     ACQFIID VARCHAR,
#     MCC_DESCRIPTION VARCHAR,
#     STAN NUMERIC,
#     PTSP_RATE NUMERIC,
#     DOCNO_TEM VARCHAR,
#     SUPERAGENTVALUE NUMERIC,
#     OPDATE2 DATE,
#     BENEFICIARYNAME VARCHAR,
#     BLRR_BANK VARCHAR,
#     BLRR_BANKCODE VARCHAR,
#     AGNT_NAME VARCHAR,
#     NET_AMOUNTDUEMERCHANT3 NUMERIC,
#     TOTALAGENTFEE NUMERIC,
#     IRF_RATE NUMERIC,
#     GOVT_VALUE NUMERIC,
#     TEXTMESS VARCHAR,
#     SETTLEMENTACCOUNT6 VARCHAR,
#     UNSHAREDMSC1_AMOUNT NUMERIC,
#     CARF NUMERIC,
#     PERC_ALLOTUPSL NUMERIC,
#     TRANNUMBER VARCHAR,
#     VEND_VALUE NUMERIC,
#     REVERSEREQID VARCHAR,
#     SETTLEMENTDATE_PREV DATE,
#     TRANDATE DATE,
#     MERCHANTDEPOSITBANKCODE3 VARCHAR,
#     MSC2_SPLITSHARINGACCT VARCHAR,
#     ACQCOUNTRY NUMERIC,
#     SETTLEMENTACCOUNT2 VARCHAR,
#     VAST_CODE VARCHAR,
#     ACQR_BANK VARCHAR,
#     VAST_RATE NUMERIC,
#     COAQ_ACCOUNT VARCHAR,
#     FEE1 NUMERIC,
#     MSCCONCESSION_RATE NUMERIC,
#     TERW_BANK VARCHAR,
#     TECH_NAME VARCHAR,
#     MASKEDPAN VARCHAR,
#     VAST_BANKCODE VARCHAR,
#     BLLR_ACCOUNT VARCHAR,
#     MERCHANTDEPOSITBANKCODE6 VARCHAR,
#     MARGIN NUMERIC,
#     SETTLEMENTDATE DATE,
#     ISSR_ACCOUNT VARCHAR,
#     TRACENO NUMERIC,
#     ISSR_VALUE NUMERIC,
#     REVW_NAME VARCHAR,
#     AGNT_ACCOUNT VARCHAR,
#     CREDITACCT VARCHAR,
#     AMOUNTDUE_ISSUER NUMERIC,
#     PROS_BANK VARCHAR,
#     MERCHANTID VARCHAR,
#     BENEFICIARYBANKCODE VARCHAR,
#     ACQR_VALUE NUMERIC,
#     PTSP_NAME VARCHAR,
#     MEB_ISSFIID VARCHAR,
#     FULLREM VARCHAR,
#     ISSFIID VARCHAR,
#     TOTAL_AMOUNTDUE NUMERIC,
#     BLLR_CODE VARCHAR,
#     PTSAVAT NUMERIC,
#     TERW_CODE VARCHAR,
#     PROS_RATE NUMERIC,
#     MSC2_RATE NUMERIC,
#     MERCHANTDEPOSITBANKCODE1 VARCHAR,
#     PERCHANTAGE1 NUMERIC,
#     NET_AMOUNTDUEMERCHANT6 NUMERIC,
#     VAST_VALUE NUMERIC,
#     PTSA_VALUE NUMERIC,
#     NET_AMOUNTDUEMERCHANT NUMERIC,
#     ORIGTYPE VARCHAR,
#     PERCHANTAGE2 NUMERIC,
#     MERCHANTDEPOSITBANK5 VARCHAR,
#     MERCHANTDEPOSITBANK3 VARCHAR,
#     EXTPAYMENTFIELDS VARCHAR,
#     PTSA_RATE NUMERIC,
#     TERW_NAME VARCHAR,
#     TECH_RATE NUMERIC,
#     DEBITACCT VARCHAR,
#     SERVICEPROVIDERBANKCODE VARCHAR,
#     ID NUMERIC,
#     SUPERAGENTACCTNAME VARCHAR,
#     AGNT_BANK VARCHAR,
#     EXPDATE DATE,
#     CLEANUPFLAG NUMERIC,
#     ACQR_ACCOUNT VARCHAR,
#     DEDUCTION_AMOUNTDUE NUMERIC,
#     MARGINVAT NUMERIC,
#     SETTLEMENTACCOUNT1 VARCHAR,
#     MERCHANTDEPOSITBANKACCTNO VARCHAR,
#     PERC_ALLOTOTHERS NUMERIC,
#     CUSTOMERACCOUNT VARCHAR,
#     CARDSCHEME VARCHAR,
#     TERW_ACCOUNT VARCHAR,
#     SUBSIDY_AMOUNT NUMERIC,
#     SETTLEMENTAMOUNT NUMERIC,
#     SWTH_RATE NUMERIC,
#     SERVICEPROVIDERBANK VARCHAR,
#     ACQR_BANKCODE VARCHAR,
#     TECH_ACCOUNT VARCHAR,
#     ACQUIRER_REF VARCHAR,
#     SETTLEMENTACCOUNT5 VARCHAR,
#     OLD_TEXTMESS VARCHAR,
#     SWTH_BANK VARCHAR,
#     PTSP_BANKCODE VARCHAR,
#     NET_AMOUNTDUEMERCHANT1 NUMERIC,
#     VEND_NAME VARCHAR,
#     NET_AMOUNTDUEMERCHANT4 NUMERIC,
#     ORIGINALAMOUNT NUMERIC,
#     FIO VARCHAR,
#     TRANCUR NUMERIC,
#     PTSA_BANKCODE VARCHAR,
#     SETTLEMENT_SERVICE VARCHAR,
#     TOACCT2 VARCHAR,
#     REPORTFLAG CHAR,
#     LCYAMOUNT NUMERIC,
#     REVW_ACCOUNT VARCHAR,
#     BLLR_BANK VARCHAR,
#     REASONFORALTERATION VARCHAR
# );
# print("--------------- RUNNING ETL FOR Settlement ----------------")
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import lit
# from pyspark.errors.exceptions.base import PySparkException
# from pyspark.sql import functions as F
# from pyspark.sql.functions import desc, row_number, monotonically_increasing_id, date_sub, expr, to_timestamp
# import psycopg2
# from pyspark.sql.window import Window
# from datetime import datetime, timedelta
# from pyspark.sql.functions import col
# import pandas as pd
# from sqlalchemy import create_engine
# import oracledb
# from datetime import timedelta
# from datetime import datetime as dt
# from pyspark.sql.functions import date_format
# from load_environments import DATAWARE_HOUSE, GENERIC_CREDENTIALS, SETTLEMENT_DETAIL_CREDENTIALS
# import cx_Oracle
# from elasticsearch import Elasticsearch
# import json
# from elasticsearch.helpers import bulk
#
#
# print(datetime.now(), "========== Start Time")
#
# # Connect to Elasticsearch
# es = Elasticsearch(['http://localhost:9200'])
#
# # Check if connected
# if es.ping():
#     print("Connected to Elasticsearch.")
# else:
#     print("Failed to connect to Elasticsearch.")
#
#
# # Index name and settings
# index_name = 'settlement_detail_index'
#
#
# # Index settings
# index_settings = {
#     'settings': {
#         'number_of_shards': 1,       # Specify the number of primary shards
#         #'number_of_replicas': 1      # Specify the number of replicas
#     }
# }
#
# # Check if index exists
# if not es.indices.exists(index=index_name):
#     # Create index with settings and mappings
#     es.indices.create(index=index_name, body=index_settings)
#     print(f"Index '{index_name}' created successfully.")
#
#
#
# # Initialize a connection to PostgreSQL
#
# # ====================
# username = DATAWARE_HOUSE['DATA_WAREHOUSE_DB_USER']
# password = DATAWARE_HOUSE['DATA_WAREHOUSE_DB_PSWD']
# host = DATAWARE_HOUSE['DATA_WAREHOUSE_DB_HOST']
# port = DATAWARE_HOUSE['DATA_WAREHOUSE_DB_PORT']
# database = DATAWARE_HOUSE['DATA_WAREHOUSE_DB_NAME']
# dwh_record_table = DATAWARE_HOUSE['DATA_WAREHOUSE_RECORD_TABLE']
# dwh_schema = DATAWARE_HOUSE['DATA_WAREHOUSE_DB_SCHEMA']
# # ====================
#
# pg_url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
# engine = create_engine(pg_url)
#
# oracle_user = SETTLEMENT_DETAIL_CREDENTIALS['SETTLEMENT_DETAIL_DATA_SOURCE_DATABASE_USER']
# oracle_password = SETTLEMENT_DETAIL_CREDENTIALS['SETTLEMENT_DETAIL_DATA_SOURCE_DATABASE_PASSWORD']
# oracle_host = SETTLEMENT_DETAIL_CREDENTIALS['SETTLEMENT_DETAIL_DATA_SOURCE_DATABASE_HOST']
# oracle_port = SETTLEMENT_DETAIL_CREDENTIALS['SETTLEMENT_DETAIL_DATA_SOURCE_DATABASE_PORT']
# oracle_service = SETTLEMENT_DETAIL_CREDENTIALS['SETTLEMENT_DETAIL_DATA_SOURCE_DATABASE_NAME']
#
#
# # Connection credentials
# dsn = f"{oracle_host}:{oracle_port}/{oracle_service}"  # Data Source Name (e.g., host:port/service_name)
#
# # Establish connection
# connection = cx_Oracle.connect(oracle_user, oracle_password, dsn)
#
# # Verify connection
# if connection:
#     print("Connected to Oracle Database!")
#
# print('=====1=====================')
# conn = psycopg2.connect(
#     dbname=database,
#     user=username,
#     password=password,
#     host=host,
#     port=port
# )
# cur = conn.cursor()
#
# dwh_db_table = f"{dwh_schema}.{SETTLEMENT_DETAIL_CREDENTIALS['SETTLEMENT_DETAIL_DATA_WARE_HOUSE_DATABASE_TABLE']}"
# cur.execute(f"select * from {dwh_db_table} order by id desc limit 1")
#
# last_record = cur.fetchone()
#
# # Calculate the date of a day before today
# current_date = datetime.now()
#
# start_date = None
# end_date = None
#
# if last_record is None:
#     start_date = current_date - timedelta(days=int(f"{GENERIC_CREDENTIALS['START_FROM_THESE_DAYS_AGO']}")) # from the past e.g: last month
#     end_date = current_date - timedelta(days=int(f"{GENERIC_CREDENTIALS['END_AT_THIS_DAY_COUNT']}")) # most recent
#
#     start_date = datetime.combine(start_date, datetime.min.time())
#     end_date = datetime.combine(end_date, datetime.max.time())
# else:
#     previous_day = datetime.now().date() - timedelta(days=int(f"{GENERIC_CREDENTIALS['END_AT_THIS_DAY_COUNT']}"))
#     start_date = datetime.combine(previous_day, datetime.min.time())
#     end_date = datetime.combine(previous_day, datetime.max.time())
#
# # SQL query to fetch rows from the source database with a specific condition
# table_to_read_from = f"{SETTLEMENT_DETAIL_CREDENTIALS['SETTLEMENT_DETAIL_DATA_SOURCE_DATABASE_SCHEMA']}.{SETTLEMENT_DETAIL_CREDENTIALS['SETTLEMENT_DETAIL_DATA_SOURCE_DATABASE_TABLE']}"
#
# # -------
# # Construct SQL query to count records between the specified dates
# sql_query1 = f"SELECT COUNT(*) AS record_count FROM {table_to_read_from} WHERE TRUNC(settlementdate) >= TO_DATE('{start_date.date()}', 'YYYY-MM-DD') AND TRUNC(settlementdate) <= TO_DATE('{end_date.date()$
#
# # Execute the SQL query to get the count of records
# record_count_result = pd.read_sql_query(sql_query1, connection)
# record_count = record_count_result.iloc[0]['RECORD_COUNT']
# print("Number of records between", start_date, "and", end_date, ":", record_count)
# # -------
#
# # SQL query to fetch rows from the source database with a specific condition
# sql_query = f"SELECT * FROM {table_to_read_from} WHERE TRUNC(settlementdate) >= TO_DATE('{start_date.date()}', 'YYYY-MM-DD') AND TRUNC(settlementdate) <= TO_DATE('{end_date.date()}', 'YYYY-MM-DD')"
#
# # Chunk size for reading from the source database
# chunk_size = 5000
#
# # Read data from the source database into a Pandas DataFrame
# df_iterator = pd.read_sql_query(sql_query, connection, chunksize=chunk_size)
#
# total_dataframe_count = 0
#
# try:
#     for df in df_iterator:
#         for col in df.columns:
#             try:
#                 df[col] = df[col].apply(lambda x: x.replace('\x00', '') if isinstance(x, str) else x)
#             except Exception as e:
#                 print(f"Error in column '{col}': {str(e)}")
#
#         df1 = df.rename(
#            columns={
#                 "ID": "department_table_id",
#         })
#
# 	df1.columns = [column.lower() for column in df1.columns]
#
#         # Load data into the destination database
#         df1.to_sql(f"{SETTLEMENT_DETAIL_CREDENTIALS['SETTLEMENT_DETAIL_DATA_WARE_HOUSE_DATABASE_TABLE']}", schema=dwh_schema, con=engine, if_exists='append', index=False)
#         total_dataframe_count += len(df1)
#         print(df1, total_dataframe_count)
#
#         # Index data into Elasticsearch
#         for index, row in df1.iterrows():
#             # Assuming each row is a document to be indexed in Elasticsearch
#             # Convert Timestamp to string representation
#
#             for column in ['trandate', 'expdate', 'cpd', 'settlementdate', 'opdate', 'time', 'createdate', 'opdate2', 'settlementdate_prev', 'reportdate']:
#                 if isinstance(row[column], pd.Timestamp):
#                     row[column] = row[column].strftime('%Y-%m-%d %H:%M:%S') if not pd.isnull(row[column]) else None
#
#
#             # Replace NaT and NaN with None
#             row = row.where(pd.notnull(row), None)
#             row = row.to_dict()
#
#
#             # Serialize the document to JSON
#             doc_json = json.dumps(row)
#             es.index(index=index_name, body=doc_json, routing="settlement_detail_route")
#
#
#     # Update the last run timestamp to the current date and time
#     print("Data loaded to the destination database.")
#     cur.execute(f"select * from {dwh_schema}.{SETTLEMENT_DETAIL_CREDENTIALS['SETTLEMENT_DETAIL_DATA_WARE_HOUSE_DATABASE_TABLE']} order by id desc limit 1")
#     last_record = cur.fetchone()
#
# except (Exception,) as err:
#     print(err)
#
# cur.execute(f"insert into {dwh_schema}.{dwh_record_table}(script_name, record) values(%s, %s)", ('settlement_detail', total_dataframe_count))
# conn.commit()
#
# if int(record_count) == int(total_dataframe_count):
#     print("Completed data transfer")
#     print(f"SQL record count: {record_count}; Dataframe Count {total_dataframe_count}")
# else:
#     print(f"Improper data transfer; SQL record count: {record_count}; Dataframe Count {total_dataframe_count}")
#
# print("END TIME OF THE SCRIPT", datetime.now())
#
#
# engine.dispose()
# #connection.dispose()
# conn.close()
#
#
