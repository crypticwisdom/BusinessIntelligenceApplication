from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status
from rest_framework.views import APIView
from django.apps import apps
from threading import Thread
from report.elastic_filter.transaction import (
    issuer_report,
    acquire_report,
    COacquire_report,
)

from report.elastic_filter.transactionemail import (
    ir,
    acquire_report as ar,
    COacquire_report as cr,
)

from report.filters.transfer import transfer_issue_filter
from report.filters.bankBranch import bankBranch
from report.filters.card import card

# from report.filters.transaction import (

# )

from report.filters.transaction_db import (
    issuer_report_db,
    acquire_report_db,
    COacquire_report_db,
)
from report.elastic_filter.card import cards

from report.filters.terminal import terminal
from datacore.modules.paginations import CustomPagination
from report.serializers.transaction import TransactionsSerializer
from report.serializers.transfer import PayarenaSerializer
from report.serializers.bankBranch import BankBranchSerializer
from report.serializers.terminal import TerminalSerializer
from report.serializers.payment import (
    CardAccountDetailsIssuingSerializer,
    HoldertagsSerializer,
)
from report.models import *
from datacore.modules.utils import (
    incoming_request_checks,
    api_response,
    get_incoming_request_checks,
    generate_csv,
    customPagination,
)
from datacore.modules.email_template import (
    generate_and_send_csv,
    generate_and_send_csv_other,
)
from django.core.serializers import serialize


# Create your views here.


def serialize_model_data(model_name, data):
    # Get the model dynamically
    Model = apps.get_model("report", model_name)

    # Get the field names from the model, excluding the 'id' field
    fields = [field.name for field in Model._meta.get_fields() if field.name != "id"]

    serialized_data = []

    for item in data:
        item_dict = {}
        for i, field in enumerate(fields):
            if i < len(item):  # Ensure we're within the bounds of the item
                item_dict[field] = item[
                    i + 1
                ]  # Adjust index to skip the first item (id)
            else:
                item_dict[field] = None  # Default value if index is out of bounds

        serialized_data.append(item_dict)

    return serialized_data


class IssuerTransaction(APIView, CustomPagination):

    def get(self, request, *args, **kwargs) -> Response:
        status_, data = get_incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )

        status_, datas = issuer_report_db(request.GET)

        if not status_:
            return Response(
                api_response(message=datas, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )

        paginated_data = self.paginate_queryset(datas, request)

        # Serialize the transaction data
        serialized_data = serialize_model_data("Transactions", paginated_data)
        result = self.get_paginated_response(serialized_data)

        return Response(
            api_response(message="Issuer report retrieved", status=True, data=result)
        )

    # def get_paginated_response(self, data):
    #     return {
    #         "count": len(data),  # Total count of items
    #         "results": data,  # Paginated results
    #     }


class DownloadIssuerTransaction(APIView, CustomPagination):

    def get(self, request, *args, **kwargs) -> Response:
        status_, data = get_incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )

        status_, data = issuer_report_db(request.GET)
        print(data)
        serialized_data = serialize_model_data("Transactions", data)

        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )
        if request.GET.get("share", None):

            Thread(
                target=generate_and_send_csv,
                args=[request, serialized_data, Transactions, request.GET.get("share")],
            ).start()
            result = Response(
                api_response(message="Please check your mail", status=True)
            )
        else:
            print(serialized_data[:10])
            result = generate_csv(serialized_data, Transactions)

        return result


class AcquireTransaction(APIView, CustomPagination):

    def get(self, request, *args, **kwargs) -> Response:
        status_, data = get_incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )

        status_, datas = acquire_report_db(request.GET)

        if not status_:
            return Response(
                api_response(message=datas, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )

        paginated_data = self.paginate_queryset(datas, request)

        # Serialize the transaction data
        serialized_data = serialize_model_data("Transactions", paginated_data)

        result = self.get_paginated_response(serialized_data)

        return Response(
            api_response(message="Acquire report retrieved", status=True, data=result)
        )


class DownloadAcquirerTransaction(APIView, CustomPagination):

    def get(self, request, *args, **kwargs) -> Response:
        status_, data = get_incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )

        status_, data = acquire_report_db(request.GET)
        print(data)
        serialized_data = serialize_model_data("Transactions", data)

        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )
        if request.GET.get("share", None):

            Thread(
                target=generate_and_send_csv,
                args=[request, serialized_data, Transactions, request.GET.get("share")],
            ).start()
            result = Response(
                api_response(message="Please check your mail", status=True)
            )
        else:
            print(serialized_data[:10])
            result = generate_csv(serialized_data, Transactions)

        return result


class COAcquireTransaction(APIView, CustomPagination):
    def get(self, request, *args, **kwargs) -> Response:
        status_, data = get_incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )

        status_, datas = COacquire_report_db(request.GET)

        if not status_:
            return Response(
                api_response(message=datas, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )

        paginated_data = self.paginate_queryset(datas, request)

        # Serialize the transaction data
        serialized_data = serialize_model_data("Transactions", paginated_data)
        result = self.get_paginated_response(serialized_data)
        return Response(
            api_response(
                message="CO-Acquire report retrieved", status=True, data=result
            )
        )


class DownloadCOAcquireTransaction(APIView, CustomPagination):
    def get(self, request, *args, **kwargs) -> Response:
        status_, data = get_incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not status_:
            return Response(
                api_response(message="unable to filter data", status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )
        status_, data = issuer_report_db(request.GET)
        print(data)
        serialized_data = serialize_model_data("Transactions", data)

        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )
        if request.GET.get("share", None):

            Thread(
                target=generate_and_send_csv,
                args=[request, serialized_data, Transactions, request.GET.get("share")],
            ).start()
            result = Response(
                api_response(message="Please check your mail", status=True)
            )
        else:
            print(serialized_data[:10])
            result = generate_csv(serialized_data, Transactions)

        return result


class TransferView(APIView, CustomPagination):
    permission_classes = []

    def get(self, request, *args, **kwargs) -> Response:
        status_, data = get_incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )
        data: dict = transfer_issue_filter(request.GET)
        paginated_data = self.paginate_queryset(data, request)

        # Serialize the transaction data
        serialized_data = serialize_model_data("PayarenaExchange", paginated_data)
        result = self.get_paginated_response(serialized_data)
        return Response(
            api_response(message="Transfer report retrieved", status=True, data=result)
        )


class DownloadTransferView(APIView, CustomPagination):
    permission_classes = []

    def get(self, request, *args, **kwargs) -> Response:
        status_, data = get_incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )
        result: dict = transfer_issue_filter(request.GET)
        if request.GET.get("share", None):

            Thread(
                target=generate_and_send_csv,
                args=[request, result, PayarenaExchange, request.GET.get("share")],
            ).start()
            result = Response(
                api_response(message="Please check your mail", status=True)
            )
        else:
            result = generate_csv(result[:7000], PayarenaExchange)

        return result


class BankBranchView(APIView, CustomPagination):
    permission_classes = []

    def get(self, request, *args, **kwargs) -> Response:
        status_, data = get_incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )

        result: dict = bankBranch(request.GET)
        paginated_data = self.paginate_queryset(result, request)

        # Serialize the transaction data
        serialized_data = serialize_model_data("Transactions", paginated_data)
        result = self.get_paginated_response(serialized_data)
        return Response(
            api_response(
                message="Bank Branch report retrieved", status=True, data=result
            )
        )


class DownloadBankBranchView(APIView, CustomPagination):
    permission_classes = []

    def get(self, request, *args, **kwargs) -> Response:
        status_, data = get_incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )

        result: dict = bankBranch(request.GET)
        if request.GET.get("share", None):
            Thread(
                target=generate_and_send_csv,
                args=[request, result, BankBranchEtl, request.GET.get("share")],
            ).start()

            result = Response(
                api_response(message="please check your mail", status=True)
            )
        else:
            result = generate_csv(result[:7000], BankBranchEtl)

        return result


class TerminalView(APIView, CustomPagination):
    permission_classes = []

    def get(self, request, *args, **kwargs) -> Response:
        status_, data = get_incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )
        result: dict = terminal(request.GET)
        paginated_data = self.paginate_queryset(result, request)

        # Serialize the transaction data
        serialized_data = serialize_model_data("Transactions", paginated_data)
        result = self.get_paginated_response(serialized_data)
        return Response(
            api_response(
                message="Bank Branch report retrieved", status=True, data=result
            )
        )


class DownloadTerminalView(APIView, CustomPagination):
    permission_classes = []

    def get(self, request, *args, **kwargs) -> Response:
        status_, data = get_incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )
        result: dict = terminal(request.GET)
        if request.GET.get("share", None):

            Thread(
                target=generate_and_send_csv,
                args=[request, result, UpConTerminalConfig, request.GET.get("share")],
            ).start()
            result = Response(
                api_response(message="Please check your mail", status=True)
            )
        else:
            result = generate_csv(result[:7000], UpConTerminalConfig)

        return result


class CardView(APIView, CustomPagination):
    permission_classes = []

    def get(self, request, *args, **kwargs) -> Response:
        status_, data = get_incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )
        test, result = card(request.GET)

        paginated_data = self.paginate_queryset(result, request)
        if test == "holdertag":
            trans: dict = HoldertagsSerializer(
                paginated_data, many=True, context={"request": request}
            ).data
        elif test == "transaction":
            trans: dict = TransactionsSerializer(
                paginated_data, many=True, context={"request": request}
            ).data

        else:
            trans: dict = CardAccountDetailsIssuingSerializer(
                paginated_data, many=True, context={"request": request}
            ).data
        result: dict = self.get_paginated_response(trans)
        return Response(
            api_response(message="card report retrieved", status=True, data=result)
        )


class DownloadCardView(APIView, CustomPagination):
    permission_classes = []

    def get(self, request, *args, **kwargs) -> Response:
        status_, data = get_incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )
        test, result = card(request.GET)

        if request.GET.get("share", None):
            if test == "holdertag":

                Thread(
                    target=generate_and_send_csv,
                    args=[request, result, Holdertags, request.GET.get("share")],
                ).start()
            elif test == "transaction":
                Thread(
                    target=generate_and_send_csv,
                    args=[request, result, Transactions, request.GET.get("share")],
                ).start()
            else:
                Thread(
                    target=generate_and_send_csv,
                    args=[
                        request,
                        result,
                        CardAccountDetailsIssuing,
                        request.GET.get("share"),
                    ],
                ).start()
            result = Response(api_response(message="check your mail", status=True))
        else:
            if test == "holdertag":
                result = generate_csv(result[:7000], Holdertags)
            elif test == "transaction":
                result = generate_csv(result[:7000], Transactions)
            else:
                result = generate_csv(result[:7000], CardAccountDetailsIssuing)

        return result
