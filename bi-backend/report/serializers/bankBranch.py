from ..models import BankBranchEtl
from rest_framework import serializers


class BankBranchSerializer(serializers.ModelSerializer):

    class Meta:
        model = BankBranchEtl
        exclude = []
