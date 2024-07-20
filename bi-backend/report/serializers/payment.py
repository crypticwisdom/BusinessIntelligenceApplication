from ..models import CardAccountDetailsIssuing,Holdertags
from rest_framework import serializers

class CardAccountDetailsIssuingSerializer(serializers.ModelSerializer):
    class Meta:
        model = CardAccountDetailsIssuing
        exclude = []

class HoldertagsSerializer(serializers.ModelSerializer):
    class Meta:
        model = Holdertags
        exclude = []