#!/bin/bash
REDIS_HOST="localhost"
REDIS_PORT="6379"
TARGET_DB="1"

# Clear the target database
redis-cli -h $REDIS_HOST -p $REDIS_PORT -n $TARGET_DB FLUSHDB

# Variables
LOGIN_URL="http://localhost:15601/account/login/"
USERNAME="sunday@tm30.net"
PASSWORD="Test12345"
API_KEY="ZPuKoTX2CohoPNC8noaiefai4lhLTi5U_PFlNvJraB5bG1mpLbWZqVjuNx6gREUA-f4"

adminCount="http://localhost:15601/account/dashboardsecondary/?reportType=adminCount"

transaction="http://localhost:15601/account/dashboardsecondary/?reportType=transaction"
mtransaction="http://localhost:15601/account/dashboardsecondary/?reportType=transaction&duration=thisMonth"
wtransaction="http://localhost:15601/account/dashboardsecondary/?reportType=transaction&duration=thisWeek"
ytransaction="http://localhost:15601/account/dashboardsecondary/?reportType=transaction&duration=thisYear"

channelCount="http://localhost:15601/account/dashboardsecondary/?reportType=channelCount"
mchannelCount="http://localhost:15601/account/dashboardsecondary/?reportType=channelCount&duration=thisMonth"
wchannelCount="http://localhost:15601/account/dashboardsecondary/?reportType=channelCount&duration=thisWeek"
ychannelCount="http://localhost:15601/account/dashboardsecondary/?reportType=channelCount&duration=thisYear"

achannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=pos"
wachannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=pos&duration=thisWeek"
machannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=pos&duration=thisMonth"
yachannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=pos&duration=thisYear"

bchannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=ussd"
wbchannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=ussd&duration=thisWeek"
mbchannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=ussd&duration=thisMonth"
ybchannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=ussd&duration=thisYear"

cchannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=atm"
wcchannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=atm&duration=thisWeek"
mcchannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=atm&duration=thisMonth"
ycchannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=atm&duration=thisYear"

dchannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=web"
wdchannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=web&duration=thisWeek"
mdchannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=web&duration=thisMonth"
ydchannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=web&duration=thisYear"

echannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=agency"
wechannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=agency&duration=thisWeek"
mechannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=agency&duration=thisMonth"
yechannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=agency&duration=thisYear"

fchannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=bankUssd"
wfchannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=bankUssd&duration=thisWeek"
mfchannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=bankUssd&duration=thisMonth"
yfchannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=bankUssd&duration=thisYear"

gchannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=mobileapp"
wgchannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=mobileapp&duration=thisWeek"
mgchannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=mobileapp&duration=thisMonth"
ygchannelAmount="http://localhost:15601/account/dashboardsecondary/?reportType=channelAmount&channel=mobileapp&duration=thisYear"


datransactionTrend="http://localhost:15601/account/dashboardsecondary/?reportType=transactionTrend&duration=daily"
wgtransactionTrend="http://localhost:15601/account/dashboardsecondary/?reportType=transactionTrend&duration=weekly"
mgtransactionTrend="http://localhost:15601/account/dashboardsecondary/?reportType=transactionTrend&duration=monthly"
ygtransactionTrend="http://localhost:15601/account/dashboardsecondary/?reportType=transactionTrend&duration=yearly"

datransactionStatus="http://localhost:15601/account/dashboardsecondary/?reportType=transactionStatus&duration=daily"
wgtransactionStatus="http://localhost:15601/account/dashboardsecondary/?reportType=transactionStatus&duration=weekly"
mgtransactionStatus="http://localhost:15601/account/dashboardsecondary/?reportType=transactionStatus&duration=monthly"
ygtransactionStatus="http://localhost:15601/account/dashboardsecondary/?reportType=transactionStatus&duration=yearly"

localForeign="http://localhost:15601/account/dashboardsecondary/?reportType=localForeign"
mlocalForeign="http://localhost:15601/account/dashboardsecondary/?reportType=localForeign&duration=thisMonth"
wlocalForeign="http://localhost:15601/account/dashboardsecondary/?reportType=localForeign&duration=thisWeek"
ylocalForeign="http://localhost:15601/account/dashboardsecondary/?reportType=localForeign&duration=thisYear"

cardProcessing="http://localhost:15601/account/dashboardsecondary/?reportType=cardProcessing"
# mcardProcessing="http://localhost:15601/account/dashboardsecondary/?reportType=cardProcessing&duration=thisMonth"
# wcardProcessing="http://localhost:15601/account/dashboardsecondary/?reportType=cardProcessing&duration=thisWeek"
# ycardProcessing="http://localhost:15601/account/dashboardsecondary/?reportType=cardProcessing&duration=thisYear"


monthlyTransaction="http://localhost:15601/account/dashboardsecondary/?reportType=monthlyTransaction"

settlementTransaction="http://localhost:15601/account/dashboardsecondary/?reportType=settlementTransaction&duration=daily"
msettlementTransaction="http://localhost:15601/account/dashboardsecondary/?reportType=settlementTransaction&duration=monthly"
wsettlementTransaction="http://localhost:15601/account/dashboardsecondary/?reportType=settlementTransaction&duration=weekly"
ysettlementTransaction="http://localhost:15601/account/dashboardsecondary/?reportType=settlementTransaction&duration=yearly"


# Perform login request
login_response=$(curl -X POST -H "Content-Type: application/json" -H "x-api-key: $API_KEY" -d "{\"requestType\":\"inbound\", \"data\":{\"email\":\"$USERNAME\",\"password\":\"$PASSWORD\"}}" $LOGIN_URL)

# Extract token from login response
echo $login_response
token=$(echo "$login_response" | grep -o '"accessToken":"[^"]*' | awk -F ':"' '{print $2}')

# Check if token is empty
if [ -z "$token" ]; then
  echo "Error: Unable to obtain token from login response"
  exit 1
fi

# Perform request to dashboard/ endpoints with token as header
adminCount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $adminCount)
echo $adminCount
transaction=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $transaction)
echo $transaction
sleep 1
mtransaction=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $mtransaction)
echo $mtransaction
sleep 1
wtransaction=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $wtransaction)
echo $wtransaction
sleep 1
ytransaction=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $ytransaction)
echo $ytransaction
sleep 1
channelCount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $channelCount)
echo $channelCount
sleep 1
mchannelCount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $mchannelCount)
echo $mchannelCount
sleep 1
wchannelCount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $wchannelCount)
echo $wchannelCount
sleep 1
ychannelCount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $ychannelCount)
echo $ychannelCount
sleep 1
achannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $achannelAmount)
echo $achannelAmount
sleep 1
wachannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $wachannelAmount)
echo $wachannelAmount
sleep 1
machannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $machannelAmount)
echo $machannelAmount
sleep 1
yachannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $yachannelAmount)
echo $yachannelAmount
sleep 1
bchannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $bchannelAmount)
echo $bchannelAmount
sleep 1
wbchannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $wbchannelAmount)
echo $wbchannelAmount
sleep 1
mbchannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $mbchannelAmount)
echo $mbchannelAmount
sleep 1
ybchannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $ybchannelAmount)
echo $ybchannelAmount
sleep 1
cchannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $cchannelAmount)
echo $cchannelAmount
sleep 1
wcchannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $wcchannelAmount)
echo $wcchannelAmount
sleep 1
mcchannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $mcchannelAmount)
echo $mcchannelAmount
sleep 1
ycchannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $ycchannelAmount)
echo $ycchannelAmount
sleep 1
dchannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $dchannelAmount)
echo $dchannelAmount
sleep 1
wdchannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $wdchannelAmount)
echo $wdchannelAmount
sleep 1
mdchannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $mdchannelAmount)
echo $mdchannelAmount
sleep 1
ydchannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $ydchannelAmount)
echo $ydchannelAmount
sleep 1
echannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $echannelAmount)
echo $echannelAmount
sleep 1
echannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $echannelAmount)
echo $echannelAmount
sleep 1
wechannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $wechannelAmount)
echo $wechannelAmount
sleep 1
mechannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $mechannelAmount)
echo $mechannelAmount
sleep 1
yechannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $yechannelAmount)
echo $yechannelAmount
sleep 1
fchannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $fchannelAmount)
echo $fchannelAmount
sleep 1
wfchannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $wfchannelAmount)
echo $wfchannelAmount
sleep 1
mfchannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $mfchannelAmount)
echo $mfchannelAmount
sleep 1
yfchannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $yfchannelAmount)
echo $yfchannelAmount
sleep 1
gchannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $gchannelAmount)
echo $gchannelAmount
sleep 1
wgchannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $wgchannelAmount)
echo $wgchannelAmount
sleep 1
mgchannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $mgchannelAmount)
echo $mgchannelAmount
sleep 1
ygchannelAmount=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $ygchannelAmount)
echo $ygchannelAmount
sleep 1
settlementTransaction=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $settlementTransaction)
echo $settlementTransaction
sleep 1
msettlementTransaction=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $msettlementTransaction)
echo $msettlementTransaction
sleep 1
wsettlementTransaction=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $wsettlementTransaction)
echo $wsettlementTransaction
sleep 1
ysettlementTransaction=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $ysettlementTransaction)
echo $ysettlementTransaction
datransactionTrend=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $datransactionTrend)
echo $datransactionTrend
sleep 1
wgtransactionTrend=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $wgtransactionTrend)
echo $wgtransactionTrend
sleep 1
mgtransactionTrend=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $mgtransactionTrend)
echo $mgtransactionTrend
sleep 1
ygtransactionTrend=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $ygtransactionTrend)
echo $ygtransactionTrend
sleep 1
datransactionStatus=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $datransactionStatus)
echo $datransactionStatus
sleep 1
wgtransactionStatus=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $wgtransactionStatus)
echo $wgtransactionStatus
sleep 1
mgtransactionStatus=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $mgtransactionStatus)
echo $mgtransactionStatus
sleep 1
ygtransactionStatus=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $ygtransactionStatus)
echo $ygtransactionStatus
sleep 1
localForeign=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $localForeign)
echo $localForeign
sleep 1
mlocalForeign=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $mlocalForeign)
echo $mlocalForeign
sleep 1
wlocalForeign=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $wlocalForeign)
echo $wlocalForeign
sleep 1
ylocalForeign=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $ylocalForeign)
echo $ylocalForeign
sleep 1
cardProcessing=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $cardProcessing)
echo $cardProcessing
sleep 1

monthlyTransaction=$(curl -H "Authorization: Bearer $token" -H "x-api-key: $API_KEY" $monthlyTransaction)
echo $monthlyTransaction
