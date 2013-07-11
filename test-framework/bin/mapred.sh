#!/bin/sh

source common.sh

#echo "Deploying AggregateMetrics"
#$APP_HOME/bin/reactor-client deploy --archive $APP_HOME/examples/AggregateMetrics/AggregateMetrics.jar
#sleep 1

#echo "Starting PurchaseHistory::PurchaseHistoryBuilder MapReduce"
$APP_HOME/bin/reactor-client start --application PurchaseHistory  --mapreduce PurchaseHistoryBuilder
#sleep 1

echo "Check status: PurchaseHistory::PurchaseHistoryBuilder "
$APP_HOME/bin/reactor-client status --application PurchaseHistory --mapReduce PurchaseHistoryBuilder
