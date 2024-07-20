from django.db import models
from django.contrib.auth.models import User
from datacore.modules.choices import ROLE_CHOICES, APPROVAL_STATUS


class Institution(models.Model):
    name = models.CharField(max_length=200)
    terminalId = models.CharField(max_length=100, blank=True, null=True)
    tlaCode = models.CharField(max_length=100, blank=True, null=True)
    bespokeCode = models.CharField(max_length=100, blank=True, null=True)
    acquireTlaCode = models.CharField(max_length=100, blank=True, null=True)
    acquireBespokeCode = models.CharField(max_length=100, blank=True, null=True)
    exchangeCode = models.CharField(max_length=100, blank=True, null=True)
    issuingCode = models.CharField(max_length=100, blank=True, null=True)
    branch = models.CharField(max_length=100, blank=True, null=True)
    bin = models.JSONField(blank=True, null=True, default=[])
    status = models.BooleanField(default=True)
    createdBy = models.ForeignKey(
        User, on_delete=models.SET_NULL, blank=True, null=True
    )
    createdOn = models.DateTimeField(auto_now_add=True, blank=True, null=True)

    def __str__(self):
        return f"{self.id} - {self.name}"


class TransactionType(models.Model):
    name = models.CharField(max_length=50)
    tlaCode = models.CharField(max_length=100, blank=True, null=True)
    bespokeCode = models.CharField(max_length=100, blank=True, null=True)
    status = models.BooleanField(default=True)
    createdOn = models.DateTimeField(auto_now_add=True, blank=True, null=True)

    def __str__(self):
        return f"{self.id} - {self.name}"


class TerminalId(models.Model):
    name = models.CharField(max_length=50)
    code = models.CharField(max_length=100, blank=True, null=True)
    status = models.BooleanField(default=True)
    createdOn = models.DateTimeField(auto_now_add=True, blank=True, null=True)

    def __str__(self):
        return f"{self.id} - {self.name}"


class Channel(models.Model):
    name = models.CharField(max_length=50)
    tlaCode = models.CharField(max_length=100, blank=True, null=True)
    bespokeCode = models.CharField(max_length=100, blank=True, null=True)
    status = models.BooleanField(default=True)
    createdOn = models.DateTimeField(auto_now_add=True, blank=True, null=True)

    def __str__(self):
        return f"{self.id} - {self.name}"


class TerminalCondition(models.Model):
    name = models.CharField(max_length=100)
    code = models.CharField(max_length=150, blank=True, null=True)
    status = models.BooleanField(default=True)
    createdOn = models.DateTimeField(auto_now_add=True, blank=True, null=True)

    def __str__(self):
        return f"{self.id} - {self.name}"


class SchemaName(models.Model):
    name = models.CharField(max_length=100)
    code = models.CharField(max_length=150, blank=True, null=True)
    status = models.BooleanField(default=True)
    createdOn = models.DateTimeField(auto_now_add=True, blank=True, null=True)

    def __str__(self):
        return f"{self.id} - {self.name}"


class Country(models.Model):
    name = models.CharField(max_length=200)
    code = models.CharField(max_length=100, blank=True, null=True)
    status = models.BooleanField(default=True)
    createdOn = models.DateTimeField(auto_now_add=True, blank=True, null=True)

    def __str__(self):
        return f"{self.id} - {self.name}"

    class Meta:
        verbose_name_plural = "Countries"


class Currency(models.Model):
    country = models.ForeignKey(Country, on_delete=models.CASCADE)
    name = models.CharField(max_length=200)
    code = models.CharField(max_length=100, blank=True, null=True)
    status = models.BooleanField(default=True)
    createdOn = models.DateTimeField(auto_now_add=True, blank=True, null=True)

    def __str__(self):
        return f"{self.id} - {self.name}"

    class Meta:
        verbose_name_plural = "Currencies"


class UserDetail(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    role = models.CharField(max_length=100, choices=ROLE_CHOICES, default="readOnly")
    phoneNumber = models.CharField(max_length=100, blank=True, null=True)
    passwordChanged = models.BooleanField(default=False)
    otp = models.TextField(blank=True, null=True)
    otpExpiry = models.DateTimeField(blank=True, null=True)
    institution = models.ForeignKey(
        Institution, on_delete=models.SET_NULL, blank=True, null=True
    )
    createdBy = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="created_by",
    )
    createdOn = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    updatedOn = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.user.username}"


class ExtraParameters(models.Model):
    name = models.CharField(max_length=200)
    bin = models.JSONField(blank=True, null=True)
    status = models.BooleanField(default=True)
    createdOn = models.DateTimeField(auto_now_add=True, blank=True, null=True)

    def __str__(self):
        return f"{self.name}"


class CardStatus(models.Model):
    name = models.CharField(max_length=200)
    value = models.CharField(max_length=200)
    status = models.BooleanField(default=True)
    createdOn = models.DateTimeField(auto_now_add=True, blank=True, null=True)

    def __str__(self):
        return f"{self.name}"


class Approval(models.Model):
    user = models.ForeignKey(
        UserDetail, on_delete=models.SET_NULL, blank=True, null=True
    )
    modelName = models.CharField(max_length=200, blank=True, null=True)
    fieldName = models.CharField(max_length=200, blank=True, null=True)
    action = models.CharField(max_length=200, blank=True, null=True)
    fieldId = models.CharField(max_length=200, blank=True, null=True)
    detail = models.CharField(max_length=200, blank=True, null=True)
    data = models.JSONField(blank=True, null=True)
    status = models.CharField(
        max_length=100, choices=APPROVAL_STATUS, default="Pending"
    )
    createdOn = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    updatedOn = models.DateTimeField(auto_now=True, blank=True, null=True)

    def __str__(self):
        return f"{self.detail}"
