from rest_framework import serializers
from ..models import PayarenaExchange

class PayarenaSerializer(serializers.ModelSerializer):
    class Meta:
        model = PayarenaExchange
        exclude = []