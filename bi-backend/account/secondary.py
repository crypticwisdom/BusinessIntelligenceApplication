from django.contrib.auth.models import User
from django.db.models import Q
from django.shortcuts import get_object_or_404
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status
from rest_framework.views import APIView

from datacore.modules.choices import COUNTRY
from datacore.modules.exceptions import raise_serializer_error_msg
from datacore.modules.paginations import CustomPagination

# from datacore.modules.elastic_report import (
# )
from datacore.modules.permissions import IsRiskAdmin, IsAdmin
from datacore.modules.reportsecondary import (
    total_transaction_count_and_value,
    total_admin_count_report,
    terminalSourceAndDestination,
    local_to_foreign_transaction_report,
    transaction_status,
    transaction_trends_report,
    monthly_transaction_report,
    settlement_report,
    transaction_by_channel_report,
    transaction_status_per_channel,
    card_processed_count,
)
from datacore.modules.utils import (
    incoming_request_checks,
    api_response,
    get_incoming_request_checks,
    approval_request,
)


class DashboardAPIView(APIView):

    def get(self, request):
        status_, data_ = get_incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data_, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )

        report_type = request.GET.get("reportType")
        inst_id = request.GET.get("institutionId", None)
        duration = request.GET.get("duration", "yesterday")
        channel = request.GET.get("channel", "ussd")

        data = dict()
        if report_type == "adminCount":
            data["adminCount"] = total_admin_count_report(request.user, inst_id)
        elif report_type == "transaction":
            data["transaction"] = total_transaction_count_and_value(
                request.user, inst_id, duration
            )
        elif report_type == "channelCount":
            data["channelCount"] = transaction_by_channel_report(
                request.user, inst_id, duration
            )
        elif report_type == "channelAmount":
            data["channelAmount"] = transaction_status_per_channel(
                request.user, inst_id, channel, duration
            )
        elif report_type == "transactionTrend":
            data["transactionTrend"] = transaction_trends_report(
                request.user, inst_id, duration
            )
        elif report_type == "transactionStatus":
            data["transactionStatus"] = transaction_status(
                request.user, inst_id, duration
            )
        elif report_type == "localForeign":
            data["localForeign"] = local_to_foreign_transaction_report(
                request.user, inst_id, duration
            )
        elif report_type == "cardProcessing":
            data["cardProcessing"] = card_processed_count(
                request.user, inst_id, duration
            )
        elif report_type == "monthlyTransaction":
            data["monthlyTransaction"] = monthly_transaction_report(
                request.user, inst_id
            )
        elif report_type == "settlementTransaction":
            data["settlementTransaction"] = settlement_report(duration)

        else:
            return Response(
                api_response(message="Selected reportType is not valid", status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )

        return Response(api_response(message="Data retrieved", status=True, data=data))
