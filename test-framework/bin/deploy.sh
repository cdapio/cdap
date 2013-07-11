#!/bin/sh

BASEDIR=$(dirname $0)

source $BASEDIR/common.sh

echo "deploying samples..."
#$APP_HOME/bin/reactor-client deploy --archive $APP_HOME/examples/AggregateMetrics/AggregateMetrics.jar
$APP_HOME/bin/reactor-client deploy --archive $APP_HOME/examples/CountCounts/CountCounts.jar 
$APP_HOME/bin/reactor-client deploy --archive $APP_HOME/examples/CountAndFilterWords/CountAndFilterWords.jar
$APP_HOME/bin/reactor-client deploy --archive $APP_HOME/examples/CountOddAndEven/CountOddAndEven.jar
$APP_HOME/bin/reactor-client deploy --archive $APP_HOME/examples/CountRandom/CountRandom.jar
$APP_HOME/bin/reactor-client deploy --archive $APP_HOME/examples/CountTokens/CountTokens.jar 
$APP_HOME/bin/reactor-client deploy --archive $APP_HOME/examples/HelloWorld/HelloWorld.jar 
$APP_HOME/bin/reactor-client deploy --archive $APP_HOME/examples/SimpleWriteAndRead/SimpleWriteAndRead.jar
$APP_HOME/bin/reactor-client deploy --archive $APP_HOME/examples/WordCount/WordCount.jar
$APP_HOME/bin/reactor-client deploy --archive $APP_HOME/examples/Purchase/PurchaseApp.jar
