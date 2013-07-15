#!/bin/sh

: ${APP_HOME?"not set."}

if [ $# -lt 1 ]
then
    echo "usage test-procedure.sh <start|stop>"
    exit 1
fi

echo "Starting/stopping all procedures..."
$APP_HOME/bin/reactor-client $1 --application CountCounts --procedure CountCountProcedure
$APP_HOME/bin/reactor-client $1 --application HelloWorld --procedure whoFlow
$APP_HOME/bin/reactor-client $1 --application PurchaseHistory  --procedure PurchaseQuery
$APP_HOME/bin/reactor-client $1 --application WordCount --procedure WordCounter
echo "All applications started"
