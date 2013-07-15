#!/bin/sh
if [ $# -lt 2 ]
then
    echo "Usage: start-stop.sh <start|stop>  <APP_HOME>"
    exit 1
fi

APP_HOME=$2

#setup environment, compile if necessary
#BASEDIR=$(dirname $0)
#source $BASEDIR/common.sh

$APP_HOME/bin/reactor-client $1 --application CountAndFilterWords  --flow CountAndFilterWords
$APP_HOME/bin/reactor-client $1 --application CountCounts --flow CountCounts
$APP_HOME/bin/reactor-client $1 --application CountOddAndEven --flow CountOddAndEven
$APP_HOME/bin/reactor-client $1 --application CountRandom --flow CountRandom
$APP_HOME/bin/reactor-client $1 --application CountTokens --flow CountTokens
$APP_HOME/bin/reactor-client $1 --application HelloWorld --flow whoFlow
$APP_HOME/bin/reactor-client $1 --application SimpleWriteAndRead --flow SimpleWriteAndRead
$APP_HOME/bin/reactor-client $1 --application WordCount --flow WordCounter
$APP_HOME/bin/reactor-client $1 --application PurchaseHistory  --flow PurchaseFlow
echo "All applications started"

