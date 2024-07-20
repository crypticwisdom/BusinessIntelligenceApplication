from rest_framework import serializers
from .models import InsightAnalysisNotificationModel, InsightConfigModel, InsightModel, InsightModel



class InsightModelSerializer(serializers.ModelSerializer):
    class Meta:
        model = InsightModel
        fields = '__all__'


class InsightAnalysisNotificationModelSerializer(serializers.ModelSerializer):
    class Meta:
        model = InsightAnalysisNotificationModel
        fields = "__all__"


class InsightConfigSerializer(serializers.ModelSerializer):
    insight_config_settings = serializers.SerializerMethodField()

    def get_insight_config_settings(self, insight_module):
        request = self.context.get('request', None)
        if request is None:
            return {}
        insight_module_config_instance = InsightConfigModel.objects.filter(insight=insight_module, institution=request.user.userdetail.institution)

        if not insight_module_config_instance.exists():
            return {}

        insight_module_config_instance = insight_module_config_instance.last()

        return {
            "slug": insight_module_config_instance.slug,
            "daily_threshold_up": insight_module_config_instance.daily_threshold_up,
            "daily_threshold_down": insight_module_config_instance.daily_threshold_down,
            "daily_bench_mark": insight_module_config_instance.daily_bench_mark,
            "daily_bench_mark_with_threshold": insight_module_config_instance.daily_bench_mark_with_threshold,

            "weekly_threshold_up": insight_module_config_instance.weekly_threshold_up,
            "weekly_threshold_down": insight_module_config_instance.weekly_threshold_down,
            "weekly_bench_mark": insight_module_config_instance.weekly_bench_mark,
            "weekly_bench_mark_with_threshold": insight_module_config_instance.weekly_bench_mark_with_threshold,
            "monthly_threshold_up": insight_module_config_instance.monthly_threshold_up,
            "monthly_threshold_down": insight_module_config_instance.monthly_threshold_down,
            "monthly_bench_mark": insight_module_config_instance.monthly_bench_mark,
            "monthly_bench_mark_with_threshold": insight_module_config_instance.monthly_bench_mark_with_threshold,

            "short_message": insight_module_config_instance.short_message,
            "long_message": insight_module_config_instance.long_message,
            "recommendation": insight_module_config_instance.recommendation,
        }

    class Meta:
        model = InsightModel
        fields = [
            "name",
            "slug",
            "admin_activated",
            "insight_config_settings",
        ]


class SetInsightConfigSerializerIn(serializers.Serializer):
    user = serializers.HiddenField(default=serializers.CurrentUserDefault())

    daily_threshold_up = serializers.DecimalField(required=False, decimal_places=2, max_digits=10)
    daily_threshold_down = serializers.DecimalField(required=False, decimal_places=2, max_digits=10)
    daily_bench_mark = serializers.DecimalField(required=False, decimal_places=2, max_digits=1000)
    daily_bench_mark_with_threshold = serializers.DecimalField(required=False, decimal_places=2, max_digits=1000)

    weekly_threshold_up = serializers.DecimalField(required=False, decimal_places=2, max_digits=10)
    weekly_threshold_down = serializers.DecimalField(required=False, decimal_places=2, max_digits=10)
    weekly_bench_mark = serializers.DecimalField(required=False, decimal_places=2, max_digits=1000)
    weekly_bench_mark_with_threshold = serializers.DecimalField(required=False, decimal_places=2, max_digits=1000)

    monthly_threshold_up = serializers.DecimalField(required=False, decimal_places=2, max_digits=10)
    monthly_threshold_down = serializers.DecimalField(required=False, decimal_places=2, max_digits=10)
    monthly_bench_mark = serializers.DecimalField(required=False, decimal_places=2, max_digits=1000)
    monthly_bench_mark_with_threshold = serializers.DecimalField(required=False, decimal_places=2, max_digits=1000)

    short_message = serializers.CharField(required=False, max_length=10000)
    long_message = serializers.CharField(required=False, max_length=10000)

    recommendation = serializers.CharField(required=False, max_length=10000)
    active = serializers.BooleanField(required=False)

    def update(self, instance, validated_data):
        daily_threshold_up = validated_data.get('daily_threshold_up', instance.daily_threshold_up)
        daily_threshold_down = validated_data.get('daily_threshold_down', instance.daily_threshold_down)
        daily_bench_mark = validated_data.get('daily_bench_mark', instance.daily_bench_mark)
        daily_bench_mark_with_threshold = validated_data.get('daily_bench_mark_with_threshold', instance.daily_bench_mark_with_threshold)

        weekly_threshold_up = validated_data.get('weekly_threshold_up', instance.weekly_threshold_up)
        weekly_threshold_down = validated_data.get('weekly_threshold_down', instance.weekly_threshold_down)
        weekly_bench_mark = validated_data.get('weekly_bench_mark', instance.weekly_bench_mark)
        weekly_bench_mark_with_threshold = validated_data.get('weekly_bench_mark_with_threshold', instance.weekly_bench_mark_with_threshold)

        monthly_threshold_up = validated_data.get('monthly_threshold_up', instance.monthly_threshold_up)
        monthly_threshold_down = validated_data.get('monthly_threshold_down', instance.monthly_threshold_down)
        monthly_bench_mark = validated_data.get('monthly_bench_mark', instance.monthly_bench_mark)
        monthly_bench_mark_with_threshold = validated_data.get('monthly_bench_mark_with_threshold', instance.monthly_bench_mark_with_threshold)

        short_message = validated_data.get('short_message', instance.short_message)
        priority = validated_data.get('priority', instance.priority)
        if priority not in ('urgent', 'important', 'critical', 'info'):
            raise serializers.ValidationError(f"'{priority}' not in choices")

        long_message = validated_data.get('long_message', instance.long_message)
        recommendation = validated_data.get('recommendation', instance.recommendation)

        active = validated_data.get('active', instance.active)

        # Assign values to 'instance'.
        instance.daily_threshold_up = daily_threshold_up
        instance.daily_threshold_down = daily_threshold_down
        instance.daily_bench_mark = daily_bench_mark
        instance.daily_bench_mark_with_threshold = daily_bench_mark_with_threshold

        instance.weekly_threshold_up = weekly_threshold_up
        instance.weekly_threshold_down = weekly_threshold_down
        instance.weekly_bench_mark = weekly_bench_mark
        instance.weekly_bench_mark_with_threshold = weekly_bench_mark_with_threshold

        instance.monthly_threshold_up = monthly_threshold_up
        instance.monthly_threshold_down = monthly_threshold_down
        instance.monthly_bench_mark = monthly_bench_mark
        instance.monthly_bench_mark_with_threshold = monthly_bench_mark_with_threshold

        instance.short_message = short_message
        instance.long_message = long_message
        instance.recommendation = recommendation
        instance.active = active

        instance.save()
        return instance

    # Added Meta so this serializer can also be used to return data
    class Meta:
        exclude = ['id', 'start_time', 'end_time', 'date_created', 'date_updated', 'result']
        model = InsightConfigModel

