from django.contrib.auth.models import User
from django.db.models import Q
from django.shortcuts import get_object_or_404
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status
from report.models import Transactions
from rest_framework.views import APIView
from rest_framework_simplejwt.tokens import RefreshToken, AccessToken
from rest_framework import generics
from account.models import (
    Institution,
    Country,
    Channel,
    Currency,
    SchemaName,
    TerminalCondition,
    UserDetail,
    TransactionType,
    TerminalId,
    CardStatus,
    Approval,
)
from datacore.modules.choices import COUNTRY
from datacore.modules.exceptions import raise_serializer_error_msg
from datacore.modules.paginations import CustomPagination

# from datacore.modules.elastic_report import (
# )
from datacore.modules.permissions import IsRiskAdmin, IsAdmin
from datacore.modules.report import (
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
from account.serializers import (
    UserSerializerIn,
    UserSerializerOut,
    LoginSerializerIn,
    ConfirmOTPSerializerIn,
    ChangePasswordSerializerIn,
    ForgotPasswordSerializerIn,
    RequestOTPSerializerIn,
    InstitutionSerializerOut,
    TerminalConditionSerializerOut,
    CurrencySerializerOut,
    CountrySerializerOut,
    ChannelSerializerOut,
    SchemaSerializerOut,
    BinSerializerOut,
    InstitutionSerializerIn,
    TerminalIdSerializerOut,
    TransactionTypeSerializerOut,
    TerminalDestinationOut,
    TerminalSourceOut,
    CardStatusSerializerOut,
    ApprovalSerializerIn,
    ApprovalSerializerOut,
)
from report.models import UpConTerminalConfig


class CreateUserAPIView(APIView):
    permission_classes = [IsAuthenticated & (IsAdmin | IsRiskAdmin)]

    def post(self, request):
        status_, data = incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )

        serializer = UserSerializerIn(data=data, context={"request": request})
        serializer.is_valid() or raise_serializer_error_msg(errors=serializer.errors)
        response = serializer.save()
        return Response(
            api_response(
                message="Account created successfully", status=True, data=response
            )
        )


class LoginAPIView(APIView):
    permission_classes = []

    def post(self, request):
        status_, data = incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )

        serializer = LoginSerializerIn(data=data, context={"request": request})
        serializer.is_valid() or raise_serializer_error_msg(errors=serializer.errors)
        user = serializer.save()
        return Response(
            api_response(
                message="Login successful",
                status=True,
                data={
                    "userData": UserSerializerOut(
                        user, context={"request": request}
                    ).data,
                    "accessToken": f"{AccessToken.for_user(user)}",
                    "refreshToken": f"{RefreshToken.for_user(user)}",
                },
            )
        )


class ConfirmOTPView(APIView):
    permission_classes = []

    def post(self, request):
        status_, data = incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )

        serializer = ConfirmOTPSerializerIn(data=data, context={"request": request})
        serializer.is_valid() or raise_serializer_error_msg(errors=serializer.errors)
        response = serializer.save()
        return Response(
            api_response(
                message="OTP verified successfully", data=response, status=True
            )
        )


class RequestOTPView(APIView):
    permission_classes = []

    def post(self, request):
        status_, data = incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )

        serializer = RequestOTPSerializerIn(data=data, context={"request": request})
        serializer.is_valid() or raise_serializer_error_msg(errors=serializer.errors)
        response = serializer.save()
        return Response(
            api_response(
                message="OTP has been sent to your email address",
                data=response,
                status=True,
            )
        )


class ChangePasswordView(APIView):
    permission_classes = []

    def post(self, request):
        status_, data = incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )

        serializer = ChangePasswordSerializerIn(data=data, context={"request": request})
        serializer.is_valid() or raise_serializer_error_msg(errors=serializer.errors)
        response = serializer.save()
        return Response(api_response(message=response, status=True))


class ForgotPasswordView(APIView):
    permission_classes = []

    def post(self, request):
        status_, data = incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )

        serializer = ForgotPasswordSerializerIn(data=data)
        serializer.is_valid() or raise_serializer_error_msg(errors=serializer.errors)
        response = serializer.save()
        return Response(api_response(message=response, status=True))


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


class InstitutionListAPIView(APIView, CustomPagination):
    permission_classes = []

    def get(self, request, pk=None):
        param = request.GET.get("paginate", "")
        if pk:
            queryset = get_object_or_404(Institution, id=pk)
            result = InstitutionSerializerOut(
                queryset, context={"request": request}
            ).data
            return Response(
                api_response(message="Institution retrieved", status=False, data=result)
            )
        queryset = Institution.objects.all().order_by("-id")
        result = InstitutionSerializerOut(
            queryset, many=True, context={"request": request}
        ).data
        if param == "true":
            queryset = self.paginate_queryset(queryset, request)
            serializer = InstitutionSerializerOut(
                queryset, many=True, context={"request": request}
            ).data
            result = self.get_paginated_response(serializer)

        return Response(
            api_response(message="Institutions retrieved", status=True, data=result)
        )

    def post(self, request, *args, **kwargs):
        status_, data = incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )

        user = UserDetail.objects.get(user=request.user)
        data.update(
            {
                "createdBy": user.id,
            }
        )

        Approval.objects.create(
            user=user,
            status="Pending",
            modelName="institution",
            detail=f"{user.user.first_name}-{user.user.last_name} want to create institution",
            data=data,
            action="create",
        )
        # serializer = InstitutionSerializerIn(data=data)
        # if serializer.is_valid():
        #     serializer.save()
        return Response(
            api_response(
                message=f"Request to create institution sent", status=True, data=data
            )
        )

        # return Response(api_response(message=serializer.errors, status=False), status=status.HTTP_400_BAD_REQUEST)

    def put(self, request, pk, *args, **kwargs):
        status_, data = incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )
        institution = get_object_or_404(Institution, pk=pk)
        user = UserDetail.objects.get(user=request.user)
        Approval.objects.create(
            user=user,
            status="pending",
            modelName="institution",
            fieldName=institution.name,
            detail=f"{request.user.first_name}-{request.user.last_name} want to edit institution",
            data=data,
            action="edit",
            fieldId=institution.id,
        )
        return Response(
            api_response(
                message=f"Request to edit {institution.name} sent",
                status=True,
                data=data,
            )
        )

        # data.update({"status":True})
        # serializer = InstitutionSerializerIn(instance=institution, data=data, partial=True)
        # if serializer.is_valid():
        #     serializer.save()
        #             Approval.objects.create(user=user,status="pending",modelName="institution",detail=f"{user.first_name}-{user.last_name} want to create institution",data=data,action="create")

        # return Response(api_response(message=f"Request to edit {institution.name} sent", status=True,data=data))
        # return Response(api_response(message=serializer.errors, status=False), status=status.HTTP_400_BAD_REQUEST)


class ListEditUsersAPIView(APIView, CustomPagination):
    # permission_classes = []
    permission_classes = [IsAuthenticated & (IsAdmin | IsRiskAdmin)]

    def get(self, request):
        status_, data_ = get_incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data_, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )

        admin_type = request.GET.get("adminType", "admin")
        search = request.GET.get("search")

        query = Q(userdetail__role=admin_type)
        if search:
            query &= (
                Q(first_name__icontains=search)
                | Q(last_name__icontains=search)
                | Q(email__icontains=search)
            )
        auth_user = get_object_or_404(UserDetail, user=request.user)
        queryset = self.paginate_queryset(User.objects.filter(query), request)
        if auth_user.institution:
            queryset = self.paginate_queryset(
                User.objects.filter(
                    query, userdetail__institution_id=auth_user.institution_id
                ),
                request,
            )
        serializer = UserSerializerOut(
            queryset, many=True, context={"request": request}
        ).data
        data = self.get_paginated_response(serializer)
        return Response(
            api_response(
                message=f"{admin_type} retrieved successfully", status=True, data=data
            )
        )

    def put(self, request, pk):
        status_, data = incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )

        acct_status = data.get("status")
        user_to_edit = get_object_or_404(User, id=pk)
        authenticated_acct = get_object_or_404(UserDetail, user=user_to_edit)
        if authenticated_acct.institution:
            user_to_edit = get_object_or_404(
                User,
                id=pk,
                userdetail__institution_id=authenticated_acct.institution_id,
            )
        if acct_status == "active":
            user_to_edit.is_active = True
        if acct_status == "inactive":
            user_to_edit.is_active = False
        user_to_edit.first_name = data.get("first_name", user_to_edit.first_name)
        user_to_edit.last_name = data.get("last_name", user_to_edit.last_name)
        authenticated_acct.phoneNumber = data.get(
            "phoneNumber", authenticated_acct.phoneNumber
        )
        authenticated_acct.save()
        user_to_edit.save()
        serializer = UserSerializerOut(user_to_edit, context={"request": request}).data

        return Response(
            api_response(
                message=f"User updated successfully", status=True, data=serializer
            )
        )


class ChannelListAPIView(generics.ListAPIView):
    permission_classes = []
    serializer_class = ChannelSerializerOut
    queryset = Channel.objects.all()

    def list(self, request, *args, **kwargs):
        serializer_context = {"request": request}
        serializer = self.serializer_class(
            self.get_queryset(), context=serializer_context, many=True
        )
        return Response(
            api_response(message="Channel retrieved", status=True, data=serializer.data)
        )

    def post(self, request, *args, **kwargs):
        status_, data = incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )
        user = UserDetail.objects.get(user=request.user)
        Approval.objects.create(
            user=user,
            status="pending",
            modelName="channel",
            detail=f"{request.user.first_name}-{request.user.last_name} want to create channel",
            data=data,
            action="create",
        )
        return Response(
            api_response(
                message="Request to create channel sent", status=True, data=data
            )
        )

    def put(self, request, pk, *args, **kwargs):
        status_, data = incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )
        channel = get_object_or_404(Channel, pk=pk)
        user = UserDetail.objects.get(user=request.user)
        Approval.objects.create(
            user=user,
            status="pending",
            modelName="channel",
            FieldName=channel.name,
            detail=f"{ request.user.first_name}-{request.user.last_name} want to edit channel",
            data=data,
            action="edit",
            fieldId=channel.id,
        )
        return Response(
            api_response(
                message=f"Request to edit {channel.name} sent", status=True, data=data
            )
        )


class SchemaListAPIView(generics.ListAPIView):
    permission_classes = []
    serializer_class = SchemaSerializerOut
    queryset = SchemaName.objects.all()

    def list(self, request, *args, **kwargs):
        serializer_context = {"request": request}
        serializer = self.serializer_class(
            self.get_queryset(), context=serializer_context, many=True
        )
        return Response(
            api_response(message="Schema retrieved", status=True, data=serializer.data)
        )

    def post(self, request, *args, **kwargs):
        status_, data = incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )
        user = UserDetail.objects.get(user=request.user)
        Approval.objects.create(
            user=user,
            status="pending",
            modelName="schema",
            detail=f"{request.user.first_name}-{request.user.last_name} want to create schema",
            data=data,
            action="create",
        )
        return Response(
            api_response(
                message="Request to create schema sent", status=True, data=data
            )
        )

    def put(self, request, pk, *args, **kwargs):
        status_, data = incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )
        schema = get_object_or_404(SchemaName, pk=pk)
        user = UserDetail.objects.get(user=request.user)
        Approval.objects.create(
            user=user,
            status="pending",
            fieldName=schema.name,
            modelName="schema",
            detail=f"{request.user.first_name}-{request.user.last_name} want to edit schema",
            data=data,
            action="edit",
            fieldId=schema.id,
        )
        return Response(
            api_response(
                message=f"Request to edit {schema.name} sent", status=True, data=data
            )
        )


class CardStatusAPIView(generics.ListAPIView):
    permission_classes = []
    serializer_class = CardStatusSerializerOut
    queryset = CardStatus.objects.all()

    def list(self, request, *args, **kwargs):
        serializer_context = {"request": request}
        serializer = self.serializer_class(
            self.get_queryset(), context=serializer_context, many=True
        )
        return Response(
            api_response(
                message="Card status retrieved", status=True, data=serializer.data
            )
        )

    def post(self, request, *args, **kwargs):
        status_, data = incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )
        user = UserDetail.objects.get(user=request.user)
        Approval.objects.create(
            user=user,
            status="pending",
            modelName="card-status",
            detail=f"{request.user.first_name}-{request.user.last_name} want to create card-status",
            data=data,
            action="create",
        )
        return Response(
            api_response(
                message="Request to create card-status sent", status=True, data=data
            )
        )

    def put(self, request, pk, *args, **kwargs):
        status_, data = incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )
        card = get_object_or_404(CardStatus, pk=pk)
        user = UserDetail.objects.get(user=request.user)
        Approval.objects.create(
            user=user,
            status="pending",
            fieldName=card.name,
            modelName="card",
            detail=f"{request.user.first_name}-{request.user.last_name} want to edit card",
            data=data,
            action="edit",
        )
        return Response(
            api_response(
                message=f"Request to edit {card.name} sent", status=True, data=data
            )
        )


class BinListAPIView(generics.ListAPIView):
    permission_classes = []
    serializer_class = BinSerializerOut

    def get_queryset(self):
        id_param = self.request.GET.get("id")
        queryset = Institution.objects.filter(id=id_param)
        return queryset

    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()
        serializer_context = {"request": request}
        serializer = self.serializer_class(
            queryset, context=serializer_context, many=True
        )
        return Response(
            api_response(
                message="Institution retrieved",
                status=True,
                data=serializer.data[0]["bin"],
            )
        )


class TerminalConditionListAPIView(generics.ListAPIView):
    permission_classes = []
    serializer_class = TerminalConditionSerializerOut
    queryset = TerminalCondition.objects.all()

    def list(self, request, *args, **kwargs):
        serializer_context = {"request": request}
        serializer = self.serializer_class(
            self.get_queryset(), context=serializer_context, many=True
        )
        return Response(
            api_response(
                message="Terminal Condition retrieved",
                status=True,
                data=serializer.data,
            )
        )


class CountryListAPIView(generics.ListAPIView):
    permission_classes = []
    serializer_class = CountrySerializerOut
    queryset = Country.objects.all()

    def list(self, request, *args, **kwargs):
        serializer_context = {"request": request}
        serializer = self.serializer_class(
            self.get_queryset(), context=serializer_context, many=True
        )
        return Response(
            api_response(message="Country retrieved", status=True, data=serializer.data)
        )

    def post(self, request, *args, **kwargs):
        status_, data = incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )
        user = UserDetail.objects.get(user=request.user)
        Approval.objects.create(
            user=user,
            status="pending",
            modelName="country",
            detail=f"{request.user.first_name}-{request.user.last_name} want to create country",
            data=data,
            action="create",
        )
        return Response(
            api_response(
                message="Request to create country sent", status=True, data=data
            )
        )

    def put(self, request, pk, *args, **kwargs):
        status_, data = incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )
        country = get_object_or_404(Country, pk=pk)
        user = UserDetail.objects.get(user=request.user)
        Approval.objects.create(
            user=user,
            status="pending",
            modelName=country.name,
            detail=f"{request.user.first_name}-{request.user.last_name} want to edit country",
            data=data,
            action="edit",
        )
        return Response(
            api_response(
                message=f"Request to edit {country.name} sent", status=True, data=data
            )
        )


class TerminalIdListAPIView(generics.ListAPIView):
    permission_classes = []
    serializer_class = TerminalIdSerializerOut
    queryset = TerminalId.objects.all()

    def list(self, request, *args, **kwargs):
        serializer_context = {"request": request}
        serializer = self.serializer_class(
            self.get_queryset(), context=serializer_context, many=True
        )
        return Response(
            api_response(
                message="Terminal Id retrieved", status=True, data=serializer.data
            )
        )


class TransactionTypeListAPIView(generics.ListAPIView):
    permission_classes = []
    serializer_class = TransactionTypeSerializerOut
    queryset = TransactionType.objects.all()

    def list(self, request, *args, **kwargs):
        serializer_context = {"request": request}
        serializer = self.serializer_class(
            self.get_queryset(), context=serializer_context, many=True
        )
        return Response(
            api_response(
                message="Transaction type retrieved", status=True, data=serializer.data
            )
        )

    def post(self, request, *args, **kwargs):
        status_, data = incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )
        user = UserDetail.objects.get(user=request.user)
        Approval.objects.create(
            user=user,
            status="pending",
            modelName="transactionType",
            detail=f"{request.user.first_name}-{request.user.last_name} want to create transactionType",
            data=data,
            action="create",
        )
        return Response(
            api_response(
                message="Request to create transactionType sent", status=True, data=data
            )
        )

    def put(self, request, pk, *args, **kwargs):
        status_, data = incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )
        transactionType = get_object_or_404(TransactionType, pk=pk)
        user = UserDetail.objects.get(user=request.user)
        Approval.objects.create(
            user=user,
            status="pending",
            modelName=transactionType.name,
            detail=f"{request.user.first_name}-{request.user.last_name} want to edit transactionType",
            data=data,
            action="edit",
        )
        return Response(
            api_response(
                message=f"Request to edit {transactionType.name} sent",
                status=True,
                data=data,
            )
        )


class CurrencyListAPIView(generics.ListAPIView):
    permission_classes = []
    serializer_class = CurrencySerializerOut
    queryset = Currency.objects.all()

    def list(self, request, *args, **kwargs):
        serializer_context = {"request": request}
        serializer = self.serializer_class(
            self.get_queryset(), context=serializer_context, many=True
        )
        return Response(
            api_response(
                message="Currency retrieved", status=True, data=serializer.data
            )
        )

    def post(self, request, *args, **kwargs):
        status_, data = incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )
        user = UserDetail.objects.get(user=request.user)
        Approval.objects.create(
            user=user,
            status="pending",
            modelName="currency",
            detail=f"{request.user.first_name}-{request.user.last_name} want to create currency",
            data=data,
            action="create",
        )
        return Response(
            api_response(
                message="Request to create currency sent", status=True, data=data
            )
        )

    def put(self, request, pk, *args, **kwargs):
        status_, data = incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )
        currency = get_object_or_404(Currency, pk=pk)
        user = UserDetail.objects.get(user=request.user)
        Approval.objects.create(
            user=user,
            status="pending",
            modelName=currency.name,
            detail=f"{request.user.first_name}-{request.user.last_name} want to edit currency",
            data=data,
            action="edit",
        )
        return Response(
            api_response(
                message=f"Request to edit {currency.name} sent", status=True, data=data
            )
        )


class Countrylistview(generics.ListAPIView):
    permission_classes = []

    def list(self, request, *args, **kwargs):
        data = COUNTRY

        # Load data into the Country model
        for country_id, code in data.items():
            country = Country.objects.create(
                name=f"{code}({country_id})", code=country_id
            )
            currency = Currency.objects.create(
                country=country, name=f"{code}({country_id})", code=country_id
            )
            country.save()
            currency.save()
        return Response(api_response(message="Country added", status=True))


class TerminalSourceAndDestinationView(generics.ListAPIView):
    permission_classes = []

    def get_queryset(self):
        value: str = self.request.GET.get("value")
        queryset: list = terminalSourceAndDestination(value.lower())
        return queryset

    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()
        return Response(
            api_response(message="terminal institution", status=True, data=queryset)
        )


class updatetime(generics.ListAPIView):
    permission_classes = []

    def list(self, request, *args, **kwargs):
        target_year = 2024

        # Update transaction_time for all records
        transactions_to_update = Transactions.objects.using("etl_db").all()
        for transaction in transactions_to_update:
            if transaction.transaction_time:
                new_transaction_time = transaction.transaction_time.replace(
                    year=target_year
                )
                transaction.transaction_time = new_transaction_time
                transaction.save()
        return Response(api_response(message="Country added", status=True))


class DataSourceManagementView(APIView, CustomPagination):
    permission_classes = [IsAuthenticated]

    def get(self, request, **args):
        status_, data_ = get_incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data_, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )

        type_of = request.GET.get("type")
        if type_of == "institution":
            institu: Institution = Institution.objects.filter(status=True)
            paginated_data = self.paginate_queryset(institu, request)
            serializer = InstitutionSerializerOut(
                instance=paginated_data, many=True, context={"request": request}
            ).data
            result: dict = self.get_paginated_response(serializer)
            return Response(
                api_response(message="issuer institution", status=True, data=result)
            )
        elif type_of == "bin":
            institu: Institution = Institution.objects.filter(status=True)
            paginated_data = self.paginate_queryset(institu, request)
            serializer = BinSerializerOut(
                instance=paginated_data, many=True, context={"request": request}
            ).data
            result: dict = self.get_paginated_response(serializer)
            return Response(api_response(message="bin data", status=True, data=result))
        elif type_of == "country":
            country: Country = Country.objects.filter(status=True)
            paginated_data = self.paginate_queryset(country, request)
            serializer = CountrySerializerOut(
                instance=country, many=True, context={"request": request}
            ).data
            result: dict = self.get_paginated_response(serializer)
            return Response(
                api_response(message="country data", status=True, data=result)
            )

        elif type_of == "schema":
            schema: SchemaName = SchemaName.objects.filter(status=True)
            paginated_data = self.paginate_queryset(schema, request)
            serializer = SchemaSerializerOut(
                instance=paginated_data, many=True, context={"request": request}
            ).data
            result: dict = self.get_paginated_response(serializer)
            return Response(
                api_response(message="schema data", status=True, data=result)
            )
        elif type_of == "currency":
            currency: Currency = Currency.objects.filter(status=True)
            paginated_data = self.paginate_queryset(currency, request)
            serializer = CurrencySerializerOut(
                instance=paginated_data, many=True, context={"request": request}
            ).data
            result: dict = self.get_paginated_response(serializer)
            return Response(
                api_response(message="currency data", status=True, data=result)
            )
        elif type_of == "channel":
            channel: Channel = Channel.objects.filter(status=True)
            paginated_data = self.paginate_queryset(channel, request)
            serializer = ChannelSerializerOut(
                instance=paginated_data, many=True, context={"request": request}
            ).data
            result: dict = self.get_paginated_response(serializer)
            return Response(
                api_response(message="currency data", status=True, data=result)
            )
        elif type_of == "transactionType":
            transaction: TransactionType = TransactionType.objects.filter(status=True)
            paginated_data = self.paginate_queryset(transaction, request)
            serializer = TransactionTypeSerializerOut(
                instance=paginated_data, many=True, context={"request": request}
            )
            result: dict = self.get_paginated_response(serializer.data).data
            return Response(
                api_response(message="transaction data", status=True, data=result)
            )
        elif type_of == "cardStatus":
            cardstatus: CardStatus = CardStatus.objects.filter(status=True)
            paginated_data = self.paginate_queryset(cardstatus, request)
            serializer = CardStatusSerializerOut(
                instance=paginated_data, many=True, context={"request": request}
            ).data
            result: dict = self.get_paginated_response(serializer)
            return Response(
                api_response(message="card status", status=True, data=result)
            )

        return Response(api_response(message="type not provided", status=False))


class PriviledgeAcces(APIView, CustomPagination):
    permission_classes = [IsAuthenticated]

    def get(self, request, **args):

        status_, data_ = get_incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data_, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )
        approves = Approval.objects.all()
        paginated_data = self.paginate_queryset(approves, request)
        serializer = ApprovalSerializerOut(
            instance=paginated_data, many=True, context={"request": request}
        ).data
        result: dict = self.get_paginated_response(serializer)
        return Response(api_response(message="All approvals", status=True, data=result))

    def put(self, request, pk, *args, **kwargs):

        status_, data_ = get_incoming_request_checks(request)
        if not status_:
            return Response(
                api_response(message=data_, status=False),
                status=status.HTTP_400_BAD_REQUEST,
            )
        try:
            approve = Approval.objects.get(pk=pk)
        except Approval.DoesNotExist:
            return Response(
                api_response(message="Approval not found", status=False),
                status=status.HTTP_404_NOT_FOUND,
            )

        if request.data["status"] == "Approved":
            approval_request(approve)
        approved = Approval.objects.get(pk=pk)
        approved.status = request.data["status"]
        approved.save()

        return Response(api_response(message=" approved", status=True))
        # return Response(api_response(message=serializer.errors, status=False), status=status.HTTP_400_BAD_REQUEST)
