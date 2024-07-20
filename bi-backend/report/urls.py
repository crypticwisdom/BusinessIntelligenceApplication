from django.urls import path
from report import views
from .util import filterdocument

app_name = "report"

urlpatterns = [
    # path('create-user/', views.CreateUserAPIView.as_view(), name="create-user"),
    path("issuer/", views.IssuerTransaction.as_view(), name="issuerreport"),
    path(
        "downloadissuer/",
        views.DownloadIssuerTransaction.as_view(),
        name="downloadissuer",
    ),
    path("acquirer/", views.AcquireTransaction.as_view(), name="acquirerreport"),
    path(
        "downloadacquirer/",
        views.DownloadAcquirerTransaction.as_view(),
        name="downloadacquire",
    ),
    path("coacquirer/", views.AcquireTransaction.as_view(), name="acquirerreport"),
    path(
        "downloadcoacquirer/",
        views.DownloadCOAcquireTransaction.as_view(),
        name="downloadacquirerreport",
    ),
    path("transfer/", views.TransferView.as_view(), name="Transferreport"),
    path(
        "downloadtransfer/",
        views.DownloadTransferView.as_view(),
        name="downloadTransferreport",
    ),
    path("terminal/", views.TerminalView.as_view(), name="Terminalreport"),
    path(
        "downloadterminal/",
        views.DownloadTerminalView.as_view(),
        name="downloadTerminalreport",
    ),
    path("card/", views.CardView.as_view(), name="CardView"),
    path("downloadcard/", views.DownloadCardView.as_view(), name="DownloadCardView"),
    path("receive/", filterdocument, name="filter"),
    path("bankBranch/", views.BankBranchView.as_view(), name="BankBranchView"),
    path(
        "downloadbankBranch/",
        views.DownloadBankBranchView.as_view(),
        name="downloadBankBranchView",
    ),
]
