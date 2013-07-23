#!/bin/sh
# example:
# hostname: alexg.continuuity.net
# apikey: 1bedb6e1f195e5390b25bfe6167d80b773685a82

if [ $# -lt 3 ]
then
    echo "usage test-promote.sh <hostname> <apikey> <APP_HOME>"
    exit 1
fi


echo "Using hostname: $1"
echo "Using API key: $2"

echo "Promote all apps, all flows..."
$APP_HOME/bin/reactor-client promote --application AggregateMetrics --host $1 --apikey $2
$APP_HOME/bin/reactor-client promote --application CountAndFilterWords --host $1 --apikey $2
$APP_HOME/bin/reactor-client promote --application CountCounts --host $1 --apikey $2
$APP_HOME/bin/reactor-client promote --application CountOddAndEven --host $1 --apikey $2
$APP_HOME/bin/reactor-client promote --application CountRandom --host $1 --apikey $2
$APP_HOME/bin/reactor-client promote --application CountTokens --host $1 --apikey $2
$APP_HOME/bin/reactor-client promote --application HelloWorld --host $1 --apikey $2
$APP_HOME/bin/reactor-client promote --application SimpleWriteAndRead --host $1 --apikey $2
$APP_HOME/bin/reactor-client promote --application WordCount --host $1 --apikey $2
echo "All applications promoted."