#!/bin/sh
if [ $# -lt 1 ]
then
    echo "Usage: testCmdLine.sh  <APP_HOME>"
    exit 1
fi

APP_HOME=$1
NUM_ITERATIONS=100

jars[0]="CountCounts/CountCounts.jar"
jars[1]="CountAndFilterWords/CountAndFilterWords.jar"
jars[2]="CountOddAndEven/CountOddAndEven.jar"
jars[3]="CountRandom/CountRandom.jar"
jars[4]="CountTokens/CountTokens.jar"
jars[5]="HelloWorld/HelloWorld.jar"
jars[6]="SimpleWriteAndRead/SimpleWriteAndRead.jar"
jars[7]="WordCount/WordCount.jar"
jars[8]="Purchase/PurchaseApp.jar"

#apps, flows
apps[0]="CountAndFilterWords" flows[0]="CountAndFilterWords"
apps[1]="CountCounts"         flows[1]="CountCounts"
apps[2]="CountOddAndEven"     flows[2]="CountOddAndEven"
apps[3]="CountRandom"         flows[3]="CountRandom"
apps[4]="CountTokens"         flows[4]="CountTokens"
apps[5]="HelloWorld"          flows[5]="whoFlow"
apps[6]="SimpleWriteAndRead"  flows[6]="SimpleWriteAndRead"
apps[7]="WordCount"           flows[7]="WordCount"
apps[8]="PurchaseHistory"     flows[8]="PurchaseFlow"

#compile all apps
compile() {
ant -f $APP_HOME/examples/build.xml
}

start_reactor() {
  $APP_HOME/bin/continuuity-reactor start
}

deploy_apps() {
 for (( i=0; i<=8; i++ )); do
    $APP_HOME/bin/reactor-client deploy --archive $APP_HOME/examples/${jars[i]}
  done
}

start_apps() {
  for ((i=0; i<=8; i++)); do
    $APP_HOME/bin/reactor-client start --application ${apps[i]} --flow ${flows[i]}
  done
}

start_procedure() {
echo "[INFO]: Starting all procedures..."
$APP_HOME/bin/reactor-client start --application CountCounts --procedure CountCountProcedure
$APP_HOME/bin/reactor-client start --application HelloWorld --procedure whoFlow
$APP_HOME/bin/reactor-client start --application PurchaseHistory  --procedure PurchaseQuery
$APP_HOME/bin/reactor-client start --application WordCount --procedure WordCounter
echo "All applications started"
}

check_status() {
for ((i=0; i<=8; i++)); do
    $APP_HOME/bin/reactor-client status --application ${apps[i]} --flow ${flow[i]}
  done
}

start_mr() {
#echo "Starting PurchaseHistory:PurchaseHistoryBuilder MapReduce"
$APP_HOME/bin/reactor-client start --application PurchaseHistory  --mapreduce PurchaseHistoryBuilder
}

check_mr_status() {
echo "Check status: PurchaseHistory::PurchaseHistoryBuilder "
$APP_HOME/bin/reactor-client status --application PurchaseHistory --mapReduce PurchaseHistoryBuilder
}

test_rest() {
  echo "Stress testing rest api. Number of iterations" ${NUM_ITERATIONS}
  for (( i=0; i<$NUM_ITERATIONS; i++ )); do
    msg=`date`;
    curl -q -d "today $msg" http://localhost:10000/stream/wordStream;
    #echo "sending: $msg"
  done
  echo "Stress run completed."
}

stop_reactor() {
echo "[INFO]: Stopping reactor"
$APP_HOME/bin/continuuity-reactor stop
}

clean_reactor() {
rm -r $APP_HOME/data/
}

start_reactor
compile
deploy_apps
#start_apps
#check_status
#start_mr
#check_mr_status
#test_rest
#stop_reactor
#clean_reactor

exit 0
