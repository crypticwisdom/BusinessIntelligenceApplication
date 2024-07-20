from django.urls import path
from . import views
from . import secondary

app_name = "account"

urlpatterns = [
    # Authentication
    path("create-user/", views.CreateUserAPIView.as_view(), name="create-user"),
    path("login/", views.LoginAPIView.as_view(), name="login"),
    path("confirm-otp/", views.ConfirmOTPView.as_view(), name="confirm-otp"),
    path("request-otp/", views.RequestOTPView.as_view(), name="request-otp"),
    path(
        "change-password/", views.ChangePasswordView.as_view(), name="change-password"
    ),
    path(
        "forgot-password/", views.ForgotPasswordView.as_view(), name="forgot-password"
    ),
    # privilage user
    path("privilege/", views.DataSourceManagementView.as_view(), name="issuer"),
    path("approval/", views.PriviledgeAcces.as_view(), name="approval"),
    path("approval/<int:pk>/", views.PriviledgeAcces.as_view(), name="approval"),
    # Dashboard
    path("dashboard/", views.DashboardAPIView.as_view(), name="dashboard"),
    path(
        "dashboardsecondary/",
        secondary.DashboardAPIView.as_view(),
        name="dashboardsecondary",
    ),
    path("institution/", views.InstitutionListAPIView.as_view(), name="institutions"),
    path(
        "institution/<int:pk>/",
        views.InstitutionListAPIView.as_view(),
        name="institutions",
    ),
    # Drop-Down
    path("country/", views.CountryListAPIView.as_view(), name="country"),
    path("country/<int:pk>/", views.CountryListAPIView.as_view(), name="country"),
    path("currency/", views.CurrencyListAPIView.as_view(), name="currency"),
    path("currency/<int:pk>/", views.CurrencyListAPIView.as_view(), name="currency"),
    path(
        "terminalcondition/",
        views.TerminalConditionListAPIView.as_view(),
        name="terminalcondition",
    ),
    path("channel/", views.ChannelListAPIView.as_view(), name="channel-list"),
    path("channel/<int:pk>/", views.ChannelListAPIView.as_view(), name="channel-list"),
    path("bins/", views.BinListAPIView.as_view(), name="bin"),
    path("schema/", views.SchemaListAPIView.as_view(), name="schema"),
    path("schema/<int:pk>/", views.SchemaListAPIView.as_view(), name="schema"),
    path("cardstatus/", views.CardStatusAPIView.as_view(), name="cardstatus"),
    path("cardstatus/<int:pk>/", views.CardStatusAPIView.as_view(), name="cardstatus"),
    path(
        "transactiontype/",
        views.TransactionTypeListAPIView.as_view(),
        name="transactiontype",
    ),
    path("terminalid/", views.TerminalIdListAPIView.as_view(), name="terminalId"),
    path(
        "transactiontype/<int:pk>/",
        views.TransactionTypeListAPIView.as_view(),
        name="transactiontype",
    ),
    path("addcountry/", views.Countrylistview.as_view(), name="country"),
    path("updateyear/", views.updatetime.as_view(), name="updatetime"),
    path(
        "terminalinstition/",
        views.TerminalSourceAndDestinationView.as_view(),
        name="terminalInstitution",
    ),
    # Admins
    path("admin/", views.ListEditUsersAPIView.as_view(), name="list-admin"),
    path(
        "admin/<int:pk>/",
        views.ListEditUsersAPIView.as_view(),
        name="edit-admin-status",
    ),
]
