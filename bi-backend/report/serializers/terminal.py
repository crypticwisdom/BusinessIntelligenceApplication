from rest_framework import serializers
from ..models import UpConTerminalConfig

class TerminalSerializer(serializers.ModelSerializer):
    class Meta:
        model = UpConTerminalConfig
        exclude = []