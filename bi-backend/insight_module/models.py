from datetime import time
from django.db import models
# from django.core.validators import MinValueValidator, MaxValueValidator
from account.models import UserDetail, Institution

PRIORITY_CHOICE = (
    (1, 'Urgent'),
    (2, 'Important'),
    (3, 'Critical'),
    (4, 'Info'),
)

INSIGHT_TYPE = (
    ('daily', 'Daily'),
    ('weekly', 'Weekly'),
    ('monthly', 'Monthly')
)


class BackendConfiguration(models.Model):
    """
        Backend configuration for running the insight.
    """
    number_of_days_to_run = models.IntegerField(
        default=1,
        help_text="Number of days to run; Default 1 day; which means insight runs every 1 day",
    )
    daily_count_days = models.IntegerField(null=True, blank=True, default=7,
                                           help_text="The total count of days used for running the analysis.")
    weekly_count_days = models.IntegerField(null=True, blank=True, default=4,
                                            help_text="The total count of week used for running the analysis.")
    monthly_count_days = models.IntegerField(null=True, blank=True, default=3,
                                             help_text="The total count of month used for running the analysis.")

    time_of_day_to_run = models.TimeField(
        default=time(4, 0), help_text="Time of the day for the insight process to run")

    def __str__(self):
        return f"This script runs every {self.number_of_days_to_run} at {self.time_of_day_to_run}"


class InsightModel(models.Model):
    """
        This model is designed for creating Insight models.
        Backend developer creates this instances.
    """
    name = models.CharField(max_length=200, null=True, blank=True)
    description = models.TextField()
    slug = models.SlugField(max_length=300, unique=True)
    admin_activated = models.BooleanField(default=True)
    date_created = models.DateTimeField(auto_now_add=True)
    date_updated = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.name}"

    class Meta:
        ordering = ['-id']
        # verbose_name = "Insight Model"
        # verbose_name_plural = "Insight Models"
        # db_table = "insight model"


class InsightConfigModel(models.Model):
    """
        This model is used for extending and holding Insight configuration.
        It is attached to the InsightModel through a foreign key, which means each insight model instance can be
        attached to many 'InsightConfigModel' model, and each 'InsightConfigModel' instance carries a unique
        configuration for a different 'institution' tied to the instance.

        I.E: Same institution in this instance can't have more than 1 instance of this model.
    """
    institution = models.ForeignKey(Institution, on_delete=models.SET_NULL, null=True)
    insight = models.ForeignKey(InsightModel, on_delete=models.SET_NULL, null=True)
    slug = models.SlugField(max_length=300, unique=True)
    insight_type = models.CharField(max_length=20, null=False, blank=False, default="daily", choices=INSIGHT_TYPE)

    daily_threshold_up = models.DecimalField(null=True, blank=True, default=0.00, decimal_places=2,
                                             max_digits=10)  # 0.00
    daily_threshold_down = models.DecimalField(null=True, blank=True, default=0.00, decimal_places=2,
                                               max_digits=10)  # 0.00
    daily_bench_mark = models.DecimalField(null=True, blank=True, default=0.00, decimal_places=2,
                                           max_digits=1000)  # 0.00
    daily_bench_mark_with_threshold = models.DecimalField(null=True, blank=True, default=0.00, decimal_places=2,
                                                          max_digits=1000)

    weekly_threshold_up = models.DecimalField(null=True, blank=True, default=0.00, decimal_places=2,
                                              max_digits=10)  # 0.00
    weekly_threshold_down = models.DecimalField(null=True, blank=True, default=0.00, decimal_places=2,
                                                max_digits=10)  # 0.00
    weekly_bench_mark = models.DecimalField(null=True, blank=True, default=0.00, decimal_places=2,
                                            max_digits=1000)  # 0.00
    weekly_bench_mark_with_threshold = models.DecimalField(null=True, blank=True, default=0.00, decimal_places=2,
                                                           max_digits=1000)  # 0.00

    monthly_threshold_up = models.DecimalField(null=True, blank=True, default=0.00, decimal_places=2,
                                               max_digits=10)  # 0.00
    monthly_threshold_down = models.DecimalField(null=True, blank=True, default=0.00, decimal_places=2,
                                                 max_digits=10)  # 0.00
    priority = models.CharField(max_length=25, null=False, default="Info", choices=PRIORITY_CHOICE)

    monthly_bench_mark = models.DecimalField(null=True, blank=True, default=0.00, decimal_places=2,
                                             max_digits=1000)  # 0.00
    monthly_bench_mark_with_threshold = models.DecimalField(null=True, blank=True, default=0.00, decimal_places=2,
                                                            max_digits=1000)  # 0.00

    # Incase user institution to spool insight analysis from start date to end date.
    # start date: 2021-02-20 to end date: 2024-02-29
    start_time = models.DateTimeField(null=True, blank=True, help_text="Incase 'institution' wants to manually spool "
                                                                       "insight analysis from start date to end date."
                                                                       "start date: 2021-02-20 to end date: 2024-02-29")
    end_time = models.DateTimeField(null=True, blank=True,
                                    help_text="Incase 'institution' wants to spool insight analysis from"
                                              " start date to end date. "
                                              "start date: 2021-02-20 to end date: 2024-02-29")
    short_message = models.TextField(
        help_text="Notification header; This can be edited by the owner of the insight.", null=True
    )
    long_message = models.TextField(
        help_text="Notification body; This carried detail of analysis; This can be edited "
                  "by the owner of the insight.", null=True)

    recommendation = models.TextField(
        help_text="Recommendation after analysis; To be added to long message;"
                  " This can be edited by the owner of the insight.", null=True
    )
    active = models.BooleanField(default=True)
    result = models.JSONField(default=dict)
    date_created = models.DateTimeField(auto_now_add=True, help_text="Date when this instance was created.")
    date_updated = models.DateTimeField(auto_now=True, help_text="Date when this instance was updated.")

    def __str__(self):
        return f"Insight Module Config: Created: {self.date_created}"


class InsightAnalysisNotificationModel(models.Model):
    """
        For each Calculation ran based on the configuration and period settings on the 'InsightConfigModel' instance,
        this model creates an instance which is used to notify a 'institution' that is attached to this instance through
         the 'InsightConfigModel' instance.
    """
    insight_config = models.ForeignKey(InsightConfigModel, on_delete=models.SET_NULL, null=True)
    slug = models.SlugField(max_length=300, unique=True)
    priority = models.CharField(max_length=25, null=False, default="Info", choices=PRIORITY_CHOICE)

    # Incase user wants to spool insight analysis from start date to end date.
    # start date: 2021-02-20 to end date: 2024-02-29
    start_time = models.DateTimeField(null=True, help_text="Incase user wants to spool insight analysis from"
                                                           "start date to end date."
                                                           "start date: 2021-02-20 to end date: 2024-02-29")
    end_time = models.DateTimeField(null=True, help_text="Incase user wants to spool insight analysis from"
                                                         " start date to end date. "
                                                         "start date: 2021-02-20 to end date: 2024-02-29")

    short_message = models.TextField(
        help_text="A copy; Short Message that was set in the 'InsightConfigModel' instance;"
                  "Notification header; This can only be edited in the 'InsightConfigModel';", null=True)

    long_message = models.TextField(
        help_text="A copy; Long Detailed Message that was set in the 'InsightConfigModel' instance;"
                  " Notification body; This carried detail of analysis;"
                  " This can only be edited in the 'InsightConfigModel';", null=True
    )
    read = models.BooleanField(default=False, help_text="`False` if user has not clicked to view this notification;"
                                                        " `True` if user has clicked to view this notification;")
    recommendation = models.TextField(
        help_text="A copy; Recommendation that was set in the 'InsightConfigModel' instance after analysis; "
                  "To be added to long message; This can be edited by the owner of the insight.",
        null=True
    )
    result = models.JSONField(default=dict, help_text="A copy of the 'InsightConfigModel' `result`")
    date_created = models.DateTimeField(auto_now_add=True)
    date_updated = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Notification Analysis for InsightConfigModel of {self.insight_config.slug}"
