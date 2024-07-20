from threading import Thread

from django.contrib.auth import authenticate
from django.contrib.auth.hashers import make_password
from django.contrib.auth.models import User, AnonymousUser
from django.utils import timezone
from rest_framework import serializers

from account.models import Institution, UserDetail, Currency, Channel, Country, TerminalCondition, SchemaName, \
    TransactionType, TerminalId, CardStatus, Approval
from datacore.modules.choices import ROLE_CHOICES
from datacore.modules.email_template import send_token_to_email, account_opening_email
from datacore.modules.exceptions import InvalidRequestException
from datacore.modules.utils import api_response, generate_random_password, log_request, get_next_minute, \
    generate_random_otp, encrypt_text, password_checker, decrypt_text
from report.models import UpConTerminalConfig


class InstitutionSerializerOut(serializers.ModelSerializer):
    class Meta:
        model = Institution
        exclude = []


class InstitutionSerializerIn(serializers.ModelSerializer):
    class Meta:
        model = Institution
        exclude = []


class CardStatusSerializerOut(serializers.ModelSerializer):
    class Meta:
        model = CardStatus
        exclude = []


class TerminalDestinationOut(serializers.ModelSerializer):
    class Meta:
        model = UpConTerminalConfig
        fields = ['destination']


class TerminalSourceOut(serializers.ModelSerializer):
    class Meta:
        model = UpConTerminalConfig
        fields = ['source']


class CurrencySerializerOut(serializers.ModelSerializer):
    class Meta:
        model = Currency
        exclude = []


class TerminalIdSerializerOut(serializers.ModelSerializer):
    class Meta:
        model = TerminalId
        exclude = []


class TransactionTypeSerializerOut(serializers.ModelSerializer):
    class Meta:
        model = TransactionType
        exclude = []


class CountrySerializerOut(serializers.ModelSerializer):
    class Meta:
        model = Country
        exclude = []


class SchemaSerializerOut(serializers.ModelSerializer):
    class Meta:
        model = SchemaName
        exclude = []


class BinSerializerOut(serializers.ModelSerializer):
    class Meta:
        model = Institution
        fields = ['bin', "name"]


class ChannelSerializerOut(serializers.ModelSerializer):
    class Meta:
        model = Channel
        exclude = []


class TerminalConditionSerializerOut(serializers.ModelSerializer):
    class Meta:
        model = TerminalCondition
        exclude = []


class UserSerializerOut(serializers.ModelSerializer):
    firstName = serializers.CharField(source="first_name")
    lastName = serializers.CharField(source="last_name")
    lastLogin = serializers.CharField(source="last_login")
    dateJoined = serializers.CharField(source="date_joined")
    role = serializers.CharField(source="userdetail.role")
    phoneNumber = serializers.CharField(source="userdetail.phoneNumber")
    passwordChanged = serializers.BooleanField(source="userdetail.passwordChanged")
    institution = serializers.SerializerMethodField()

    def get_institution(self, obj):
        user_detail = UserDetail.objects.get(user=obj)
        inst = None
        if user_detail.institution:
            inst = InstitutionSerializerOut(user_detail.institution).data
        return None

    class Meta:
        model = User
        exclude = [
            "is_staff", "is_superuser", "password", "first_name", "last_name", "groups",
            "user_permissions", "last_login", "date_joined"
        ]


class UserSerializerIn(serializers.Serializer):
    firstName = serializers.CharField()
    lastName = serializers.CharField()
    emailAddress = serializers.EmailField()
    phoneNumber = serializers.CharField(max_length=15, required=False)
    role = serializers.ChoiceField(choices=ROLE_CHOICES)
    institutionId = serializers.CharField(required=False)
    auth_user = serializers.HiddenField(default=serializers.CurrentUserDefault())

    def create(self, validated_data):
        first_name = validated_data.get("firstName")
        last_name = validated_data.get("lastName")
        email_address = validated_data.get("emailAddress")
        phone_number = validated_data.get("phoneNumber")
        acct_type = validated_data.get("role")
        institution_id = validated_data.get("institutionId")
        request_user = validated_data.get("auth_user")
        success = True
        message, inst = None, None

        logged_in_user = UserDetail.objects.get(user=request_user)

        # Check if role is "risk"
        if acct_type == "risk":
            message, success = "You are not permitted to perform this action: RISK ADMIN CREATION", False

        if logged_in_user.role == "admin" and logged_in_user.institution \
                is not None and (acct_type == "privileged" or acct_type == "helpdesk"):
            message, success = "You are not permitted to perform this action: PRIVILEGED/HELPDESK ADMIN CREATION", False

        # Check if user with email exists
        if User.objects.filter(email__iexact=email_address).exists():
            message, success = "User with this email address already exist", False

        # Check Institution
        if logged_in_user.institution is not None:
            inst = logged_in_user.institution
        elif institution_id:
            try:
                inst = Institution.objects.get(id=institution_id)
            except Institution.DoesNotExist:
                message, success = "The selected institution is not available/found", False
        else:
            ...

        if success is False:
            response = api_response(message=message, status=False)
            raise InvalidRequestException(response)

        # Generate random password
        random_password = generate_random_password()
        log_request(f"random password: {random_password}")

        # Create user
        user = User.objects.create(
            username=email_address, email=email_address, first_name=first_name, last_name=last_name,
            password=make_password(random_password)
        )

        user_profile = UserDetail.objects.create(
            user=user, role=acct_type, phoneNumber=phone_number, institution=inst, createdBy=logged_in_user.user
        )

        # Send OTP to user
        Thread(target=account_opening_email, args=[user_profile, str(random_password)]).start()
        return UserSerializerOut(user, context={"request": self.context.get("request")}).data


class LoginSerializerIn(serializers.Serializer):
    email = serializers.EmailField()
    password = serializers.CharField()

    def create(self, validated_data):
        email = validated_data.get("email")
        password = validated_data.get("password")

        user = authenticate(username=email, password=password)

        if not user:
            response = api_response(message="Invalid email or password", status=False)
            raise InvalidRequestException(response)

        user_profile = UserDetail.objects.get(user=user)
        if not user_profile.passwordChanged:
            # # OTP Timeout
            # expiry = get_next_minute(timezone.now(), 15)
            # random_otp = generate_random_otp()
            # log_request(random_otp)
            # encrypted_otp = encrypt_text(random_otp)
            # user_profile.otp = encrypted_otp
            # user_profile.otpExpiry = expiry
            # user_profile.save()
            #
            # Send OTP to user
            Thread(target=send_token_to_email, args=[user_profile]).start()
            response = api_response(message="Kindly change your default password", status=False,
                                    data={"passwordChanged": False, "userId": user.id})
            raise InvalidRequestException(response)

        return user


class ConfirmOTPSerializerIn(serializers.Serializer):
    userId = serializers.CharField(required=False)
    otp = serializers.CharField()

    def create(self, validated_data):
        user_id = validated_data.get("userId")
        otp = validated_data.get("otp")

        auth_user = self.context.get("request").user

        try:
            if not auth_user.is_authenticated:
                user_detail = UserDetail.objects.get(user_id=user_id)
            else:
                user_detail = UserDetail.objects.get(user=auth_user)

        except UserDetail.DoesNotExist:
            response = api_response(message="User not found", status=False)
            raise InvalidRequestException(response)

        if otp != decrypt_text(user_detail.otp):
            response = api_response(message="Invalid OTP", status=False)
            raise InvalidRequestException(response)

        # If OTP has expired
        if timezone.now() > user_detail.otpExpiry:
            response = api_response(message="OTP has expired, kindly request for another one", status=False)
            raise InvalidRequestException(response)

        return UserSerializerOut(user_detail.user, context={"request": self.context.get("request")}).data


class RequestOTPSerializerIn(serializers.Serializer):
    email = serializers.EmailField(required=False)

    def create(self, validated_data):
        email = validated_data.get("email")

        try:
            user_detail = UserDetail.objects.get(user__email=email)
        except UserDetail.DoesNotExist:
            response = api_response(message="User not found", status=False)
            raise InvalidRequestException(response)

        expiry = get_next_minute(timezone.now(), 15)
        random_otp = generate_random_otp()
        log_request(random_otp)
        encrypted_otp = encrypt_text(random_otp)
        user_detail.otp = encrypted_otp
        user_detail.otpExpiry = expiry
        user_detail.save()

        # Send OTP to user
        Thread(target=send_token_to_email, args=[user_detail]).start()
        return {"userId": user_detail.user_id}


class ChangePasswordSerializerIn(serializers.Serializer):
    userId = serializers.CharField(required=False)
    otp = serializers.CharField(required=False)
    oldPassword = serializers.CharField()
    newPassword = serializers.CharField()
    confirmPassword = serializers.CharField()

    def create(self, validated_data):
        user_id = validated_data.get("userId")
        old_password = validated_data.get("oldPassword")
        otp = validated_data.get("otp")
        new_password = validated_data.get("newPassword")
        confirm_password = validated_data.get("confirmPassword")

        user = self.context.get("request").user

        if not user.is_authenticated:
            try:
                user = User.objects.get(id=user_id)
            except User.DoesNotExist:
                response = api_response(message="User not found", status=False)
                raise InvalidRequestException(response)

        if not user.userdetail.passwordChanged and not otp:
            response = api_response(message="OTP is required to change password for the first time", status=False)
            raise InvalidRequestException(response)

        if otp:
            # Validate
            if otp != decrypt_text(user.userdetail.otp):
                response = api_response(message="Invalid OTP", status=False)
                raise InvalidRequestException(response)

            # If OTP has expired
            if timezone.now() > user.userdetail.otpExpiry:
                response = api_response(message="OTP has expired, kindly request for another one", status=False)
                raise InvalidRequestException(response)

        if not user.check_password(old_password):
            response = api_response(message="Old password is not valid", status=False)
            raise InvalidRequestException(response)

        success, text = password_checker(password=new_password)
        if not success:
            response = api_response(message=text, status=False)
            raise InvalidRequestException(response)

        # Check if newPassword and confirmPassword match
        if new_password != confirm_password:
            response = api_response(message="Passwords mismatch", status=False)
            raise InvalidRequestException(response)

        # Check if new and old passwords are the same
        if old_password == new_password:
            response = api_response(message="Old and New Passwords cannot be the same", status=False)
            raise InvalidRequestException(response)

        user.password = make_password(password=new_password)
        user.userdetail.passwordChanged = True
        user.save()
        user.userdetail.save()

        return "Password Changed Successfully"


class ForgotPasswordSerializerIn(serializers.Serializer):
    email = serializers.EmailField()
    otp = serializers.CharField()
    newPassword = serializers.CharField()
    confirmPassword = serializers.CharField()

    def create(self, validated_data):
        email = validated_data.get('email')
        otp = validated_data.get('otp')
        password = validated_data.get('newPassword')
        confirm_password = validated_data.get('confirmPassword')

        try:
            user_detail = UserDetail.objects.get(user__email=email)
        except UserDetail.DoesNotExist:
            response = api_response(message="User not found", status=False)
            raise InvalidRequestException(response)

        if timezone.now() > user_detail.otpExpiry:
            response = api_response(message="OTP has expired, Please request for another one", status=False)
            raise InvalidRequestException(response)

        if otp != decrypt_text(user_detail.otp):
            response = api_response(message="Invalid OTP", status=False)
            raise InvalidRequestException(response)

        success, msg = password_checker(password=password)
        if not success:
            raise InvalidRequestException(api_response(message=msg, status=False))

        if password != confirm_password: 
            raise InvalidRequestException(api_response(message="Passwords does not match", status=False))

        user_detail.user.password = make_password(password)
        user_detail.user.save()

        return "Password reset successful"


class ApprovalSerializerOut(serializers.ModelSerializer):
    username = serializers.SerializerMethodField()
    role = serializers.SerializerMethodField()



    def get_username(self, obj):
        # Fetch the first and last name from the related UserDetail instance
        if obj.user:
            return f"{obj.user.user.first_name} {obj.user.user.last_name}"
        return None

    def get_role(self, obj):
        # Fetch the role from the related UserDetail instance
        if obj.user:
            return obj.user.role
        return None
    class Meta:
        model = Approval
        exclude = ['user']

class ApprovalSerializerIn(serializers.Serializer):
    class Meta:
        model = Approval
        fields = '__all__'
