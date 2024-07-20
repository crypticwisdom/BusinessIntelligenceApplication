
from celery import shared_task
from datetime import date
from datetime import datetime, date, timedelta
from datacore.modules.report import get_report_data
from .models import Institution, Channel


@shared_task(bind=True)
def generate_dashboard_data():
    report_types = ["channelCount", "transaction", "adminCount", "channelAmount", "transactionStatus", "localForeign",
                    # List your report types here
                    "cardProcessing", "monthlyTransaction", "settlementTransaction", "transactionTrend",]
    # Add all possible durations here
    durations = ["yesterday", "last_week", "last_month"]
    institution_ids = Institution.objects.filter(status=True).values_list(
        "id")  # Add all possible institution IDs here
    channels = Channel.objects.filter(status=True).values_list("id")

    data = dict()
    # celery
    for report_type in report_types:
        for duration in durations:
            for inst_id in institution_ids:
                for channel in channels:
                    data_key = f"{report_type}_{duration}_{inst_id}_{
                        channel}"  # Generating a unique key
                    data[data_key] = get_report_data(
                        report_type, duration, inst_id, channel)
    return data
