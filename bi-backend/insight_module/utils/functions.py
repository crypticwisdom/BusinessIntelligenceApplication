from report.models import Transactions
from datetime import datetime
from insight_module.models import BackendConfiguration, InsightConfigModel
import datetime
from django.db.models import Avg, Sum
from decimal import Decimal, ROUND_HALF_UP


def transactions_average(**kwargs) -> (bool, dict):
    """
        ===========================
        / Algorithm Process Flow: /
        ===========================

        Note: We have 2 processes to users:
            a) For U.P 'risk' user role.
            b) For each Institution's Admin Role.

        1. Fetch all institution's admin roles.
        2. Get all institutions.
        3. Filter transactions record by 'processing' and 'bespoke' for a day before.
        4. Aggregate the total amount
        5. Fin
        Transaction Value Anomaly Detection:

    """
    request = kwargs.get('request', None)
    if request is None:
        return False, {"message": "'request' argument not passed"}

    # get the current user's institution
    institution_instance = request.user.userdetail.institution
    insight_module_config_query = InsightConfigModel.objects.only('institution').filter(
        institution=institution_instance
    )

    if not insight_module_config_query.exists():
        return False, {"message": "This institution does not have 'insight configuration'"}

    insight_module_config = insight_module_config_query.last()

    configuration_model = BackendConfiguration.objects.filter()
    day_range_for_daily_insight: int = configuration_model.last().daily_count_days

    # Make date
    today = datetime.datetime.now()
    day = datetime.timedelta(days=1)

    previous_day = today - day
    print(previous_day, "=========")

    transaction_query_set = Transactions.objects.using('etl_db').only(
        'amount',
        'transaction_time',
        'department',
        'acquirer_institution_id',
        'issuer_institution_id'
    ).filter(
        transaction_time__date="2024-10-19",
        # department="processing",
        # acquirer_institution_id=22,
        # issuer_institution_id=46
    )

    """
        Get a queryset list of amount 'value' and then aggregate by summing the returned values, a dictionary is 
        returned, then use the '.get()' method to access the 'amount' in the returned dictionary or return None if 
        'amount' is not found in the dictionary returned.
    """
    every_day_amount_sum = transaction_query_set.values_list('amount', flat=True).aggregate(
        amount=Sum('amount')
    ).get('amount', None)

    if every_day_amount_sum is None:
        return False, {"message": f"Aggregation returned 'None'"}

    # Get the average by dividing it by the Configuration value stored in 'BackendConfiguration.daily_count_days' field.
    # Round the value up to two decimal places using the .quantize() method.
    today_average_or_bench_mark = Decimal(every_day_amount_sum / day_range_for_daily_insight).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

    previous_bench_mark = insight_module_config.daily_bench_mark

    insight_module_config.daily_bench_mark = today_average_or_bench_mark

    #######################################################
    # Calculate Positive Anomaly; the threshold up value  #
    #######################################################

    # The user's set-threshold X today's calculated average (benchmark); 10% X 100 (benchmark=100) = 10
    today_thresh_hold_up_calc_with_benchmark = Decimal(insight_module_config.daily_threshold_up / 100) * today_average_or_bench_mark

    # sum, today's bench to 'today_threshold_up_calc_with_benchmark=10'; 100 + 10 = 110 (calc_benchmark_with_threshold)
    bench_mark_and_today_thresh_hold_up_calc = today_average_or_bench_mark + today_thresh_hold_up_calc_with_benchmark

    # compare (previous_bench_mark) yesterday's benchmark to "bench_mark_and_today_thresh_hold_up_calc".
    if previous_bench_mark > bench_mark_and_today_thresh_hold_up_calc:
        # Anomaly detected; Notify users.
        ...
    else:
        ...
        # Anomaly not detected.

    #########################################################
    # Calculate Negative Anomaly; the threshold down value  #
    #########################################################

    # The user's set-threshold X today's calculated average (benchmark); 10% X 100 (benchmark=100) = 10
    today_thresh_hold_down_calc_with_benchmark = (insight_module_config.daily_threshold_down / 100) * today_average_or_bench_mark

    # Minus, today's bench to today_threshold_down_calc_with_benchmark=10; 100 - 10 = 90 (calc_benchmark_with_threshold)
    bench_mark_and_today_thresh_hold_down_calc = today_average_or_bench_mark - today_thresh_hold_up_calc_with_benchmark

    # compare (previous_bench_mark) yesterday's benchmark to "today_thresh_hold_down_calc_with_benchmark".
    if previous_bench_mark < today_thresh_hold_down_calc_with_benchmark:
        # Anomaly detected
        ...
    else:
        # Not detected
        ...

    # Note: You can chain these functions together but, you need to chain them in the right order that suites your query
    # .values('field'): returns a query set of dictionaries, where the 'field' is the key while the value of the field is the dictionary's key value.
    # values_list('field',  flat=False): these returns a query set of object's field values specified in the method. By
    # default the .values_list()'s 'flat' argument is False by default, which makes it return a list of tuple of the values of field specified.
    # when flat=True, it returns jsut a query set list of tha values for the fild specified, this function can help to avoid looping through a
    # query set.
    # You can specify more than 1 field in the .values_list('field1', 'field2', flat=False) but this doesn't work when 'flat=True'.
    # 2. .aggregate(field=Sum('field'))
    # 3. .only('field1', 'field2', ...): The fields specified are loaded immediately, and the rest of the fields will be deferred until accessed.
    # 4. .defer('field1', 'field2'): The fields specified are not loaded immediately until they are accessed, and the rest of the fields will be loaded immediately and be made available.
    # Save today's daily threshold at the end of the analysis rather than before the end of the analysis.
    # insight_module_config.save()
    return True, {"message": 'transaction_query_set'}



    # for day in range(1, day_range_for_daily_insight + 1):
    #     previous_day = datetime.datetime.now() - datetime.timedelta(days=day)
    #     fd += Transactions.objects.using('etl_db').only('amount', 'transaction_time').filter(transaction_time__date=previous_day.date()).aggregate(amount=Sum('amount'))

