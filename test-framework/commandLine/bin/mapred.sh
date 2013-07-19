#!/bin/sh

#!/bin/sh
if [ $# -lt 1 ]
then
    echo "Usage: mapred.sh  <APP_HOME>"
    exit 1
fi

APP_HOME=$1

#echo "Deploying AggregateMetrics"
#$APP_HOME/bin/reactor-client deploy --archive $APP_HOME/examples/AggregateMetrics/AggregateMetrics.jar
#sleep 1

#echo "Starting PurchaseHistory::PurchaseHistoryBuilder MapReduce"
$APP_HOME/bin/reactor-client start --application PurchaseHistory  --mapreduce PurchaseHistoryBuilder
#sleep 1

echo "Check status: PurchaseHistory::PurchaseHistoryBuilder "
$APP_HOME/bin/reactor-client status --application PurchaseHistory --mapReduce PurchaseHistoryBuilder
