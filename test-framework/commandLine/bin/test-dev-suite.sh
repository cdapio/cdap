#!/bin/sh
if [ $# -lt 1 ]
then
    echo "Usage: testCmdLine.sh  <APP_HOME> <num_iterations>"
    exit 1
fi

APP_HOME=$1
BASEDIR=$(dirname $0)

#compile all apps
ant -f $APP_HOME/examples/build.xml

# start reactor
$APP_HOME/bin/continuuity-reactor restart
echo "[INFO]: Deploying all Apps"
# deploy all apps
$APP_HOME/bin/reactor-client deploy --archive $APP_HOME/examples/CountCounts/CountCounts.jar
$APP_HOME/bin/reactor-client deploy --archive $APP_HOME/examples/CountAndFilterWords/CountAndFilterWords.jar
$APP_HOME/bin/reactor-client deploy --archive $APP_HOME/examples/CountOddAndEven/CountOddAndEven.jar
$APP_HOME/bin/reactor-client deploy --archive $APP_HOME/examples/CountRandom/CountRandom.jar
$APP_HOME/bin/reactor-client deploy --archive $APP_HOME/examples/CountTokens/CountTokens.jar
$APP_HOME/bin/reactor-client deploy --archive $APP_HOME/examples/HelloWorld/HelloWorld.jar
$APP_HOME/bin/reactor-client deploy --archive $APP_HOME/examples/SimpleWriteAndRead/SimpleWriteAndRead.jar
$APP_HOME/bin/reactor-client deploy --archive $APP_HOME/examples/WordCount/WordCount.jar
$APP_HOME/bin/reactor-client deploy --archive $APP_HOME/examples/Purchase/PurchaseApp.jar

# Starting all Apps
echo "[INFO]: Starting all Apps"
$APP_HOME/bin/reactor-client start --application CountAndFilterWords  --flow CountAndFilterWords
$APP_HOME/bin/reactor-client start --application CountCounts --flow CountCounts
$APP_HOME/bin/reactor-client start --application CountOddAndEven --flow CountOddAndEven
$APP_HOME/bin/reactor-client start --application CountRandom --flow CountRandom
$APP_HOME/bin/reactor-client start --application CountTokens --flow CountTokens
$APP_HOME/bin/reactor-client start --application HelloWorld --flow whoFlow
$APP_HOME/bin/reactor-client start --application SimpleWriteAndRead --flow SimpleWriteAndRead
$APP_HOME/bin/reactor-client start --application WordCount --flow WordCounter
$APP_HOME/bin/reactor-client start --application PurchaseHistory  --flow PurchaseFlow

echo "[INFO]: Starting all procedures..."
$APP_HOME/bin/reactor-client $1 --application CountCounts --procedure CountCountProcedure
$APP_HOME/bin/reactor-client $1 --application HelloWorld --procedure whoFlow
$APP_HOME/bin/reactor-client $1 --application PurchaseHistory  --procedure PurchaseQuery
$APP_HOME/bin/reactor-client $1 --application WordCount --procedure WordCounter
echo "All applications started"


# Check all Apps statuses
echo "[INFO]: Check for apps status"
$APP_HOME/bin/reactor-client status --application CountAndFilterWords  --flow CountAndFilterWords
$APP_HOME/bin/reactor-client status --application CountCounts --flow CountCounts
$APP_HOME/bin/reactor-client status --application CountOddAndEven --flow CountOddAndEven
$APP_HOME/bin/reactor-client status --application CountRandom --flow CountRandom
$APP_HOME/bin/reactor-client status --application CountTokens --flow CountTokens
$APP_HOME/bin/reactor-client status --application HelloWorld --flow whoFlow
$APP_HOME/bin/reactor-client status --application SimpleWriteAndRead --flow SimpleWriteAndRead
$APP_HOME/bin/reactor-client status --application WordCount --flow WordCounter

#echo "Starting PurchaseHistory:PurchaseHistoryBuilder MapReduce"
$APP_HOME/bin/reactor-client start --application PurchaseHistory  --mapreduce PurchaseHistoryBuilder
sleep 10

echo "Check status: PurchaseHistory::PurchaseHistoryBuilder "
$APP_HOME/bin/reactor-client status --application PurchaseHistory --mapReduce PurchaseHistoryBuilder

#Stress Test rest api
echo "[INFO]: Start Stress, number of iterations: "$2
for (( i=0; i<$2; i++ )); do
msg=`date`;
curl -q -d "today $msg" http://localhost:10000/stream/wordStream;
echo "sending: $msg"
done
echo "Stress run completed."

@echo "[INFO]: Stopping reactor
$APP_HOME/bin/continuuity-reactor stop

#clean reactor
echo "[INFO]: cleaning reactor"
rm -r $APP_HOME/data

exit 0
