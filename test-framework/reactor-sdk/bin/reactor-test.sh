#!/bin/bash

NUM_APPS=9
jars[0]="CountCounts/CountCounts.jar"
jars[1]="CountAndFilterWords/CountAndFilterWords.jar"
jars[2]="CountOddAndEven/CountOddAndEven.jar"
jars[3]="CountRandom/CountRandom.jar"
jars[4]="CountTokens/CountTokens.jar"
jars[5]="HelloWorld/HelloWorld.jar"
jars[6]="SimpleWriteAndRead/SimpleWriteAndRead.jar"
jars[7]="WordCount/WordCount.jar"
jars[8]="Purchase/PurchaseApp.jar"

#apps
apps[0]="CountAndFilterWords"
apps[1]="CountCounts"
apps[2]="CountOddAndEven"
apps[3]="CountRandom"
apps[4]="CountTokens"
apps[5]="HelloWorld"
apps[6]="SimpleWriteAndRead"
apps[7]="WordCount"
apps[8]="PurchaseHistory"

#flows
flows[0]="CountAndFilterWords"
flows[1]="CountCounts"
flows[2]="CountOddAndEven"
flows[3]="CountRandom"
flows[4]="CountTokens"
flows[5]="whoFlow"
flows[6]="SimpleWriteAndRead"
flows[7]="WordCounter"
flows[8]="PurchaseFlow"

#compile all apps
compile() {
  mvn -f $1/examples/pom.xml
}

#deploy all apps
deploy() {
  if [ "$#" -ne 1 ] ; then
     echo "Usage: $0 deploy [APP_HOME]" >&2
     exit 1
  fi

  for i in `seq $NUM_APPS`;
  do
    $1/bin/reactor-client deploy --archive $1/examples/${jars[i]}
  done
}

# Start all apps, all flows
start() {
  echo "[INFO]: Starting all flows..."
  for i in `seq $NUM_APPS`; do
    $1/bin/reactor-client start --application ${apps[i]} --flow ${flows[i]}
  done

  echo "[INFO]: Starting all procedures..."
  $1/bin/reactor-client start --application CountCounts --procedure CountCountProcedure
  $1/bin/reactor-client start --application HelloWorld --procedure whoFlow
  $1/bin/reactor-client start --application PurchaseHistory  --procedure PurchaseQuery
  $1/bin/reactor-client start --application WordCount --procedure WordCounter
}

status() {
  for i in `seq $NUM_APPS`;
  do
    $1/bin/reactor-client status --application ${apps[i]} --flow ${flows[i]}
  done
}

start_mr() {
  $1/bin/reactor-client start --application PurchaseHistory  --mapreduce PurchaseHistoryBuilder
}

mr_status() {
  $1/bin/reactor-client status --application PurchaseHistory --mapReduce PurchaseHistoryBuilder
}

stream() {
  if [ "$#" -ne 2 ] ; then
    echo "Usage: $0 stream [hostname] [#iterations]" >&2
    exit 1
  fi

  echo $1

  for i in `seq $2`;
  do
    msg=`date`;
    #WordCount
    curl -q -d "today $msg" http://$1:10000/stream/wordStream;
    #CountToken
    curl -q -d "today $msg" http://$1:10000/stream/text;
    num=$(echo "scale=2; $i*100/$2" | bc)
    echo -ne "$num% Completed \r"
  done

  echo 'Completed.'
}

multiClientStream() {
  if [ "$#" -ne 3 ] ; then
    echo "Usage: $0 multiClientStream [hostname] [#iterations] [#clients]" >&2
    exit 1
  fi

  set -m # Enable Job Control

  for i in `seq $3`; do # start  jobs in parallel
    ./dev-suite.sh stream $1 $2 &
  done

  # Wait for all parallel jobs to finish
  while [ 1 ]; do fg 2> /dev/null; [ $? == 1 ] && break; done
}

proc() {
   if [ "$#" -ne 2 ] ; then
        echo "Usage: $0 proc [hostname] [#iterations]" >&2
        exit 1
   fi

   for i in `seq $2`;
   do
    RETVAL1=`curl -s -X POST http://$1:10010/procedure/WordCount/RetrieveCounts/getCount --data {"word":"today"}`
    RETVAL2=`curl -s -X POST http://$1:10010/procedure/WordCount/RetrieveCounts/getStats --data {}`
    num=$(echo "scale=2; $i*100/$2" | bc)
    echo -ne "$num% Completed\r"
   done
}

promote() {
if [ "$#" -ne 3 ]
then
    echo "Usage: $0 promote [hostname] [api_key]" >&2
    exit 1
fi
  echo "[INFO]: Promoting apps..."
  $1/bin/reactor-client promote --application AggregateMetrics --host $2 --apikey $3
  $1/bin/reactor-client promote --application CountAndFilterWords --host $2 --apikey $3
  $1/bin/reactor-client promote --application CountCounts --host $2 --apikey $3
  $1/bin/reactor-client promote --application CountOddAndEven --host $1 --apikey $3
  $1/bin/reactor-client promote --application CountRandom --host $2 --apikey $3
  $1/bin/reactor-client promote --application CountTokens --host $2 --apikey $3
  $1/bin/reactor-client promote --application HelloWorld --host $2 --apikey $2
  $1/bin/reactor-client promote --application SimpleWriteAndRead --host $2 --apikey $3
  $1/bin/reactor-client promote --application WordCount --host $2 --apikey $3
  echo "[INFO]: All applications promoted to $2"
}

: ${APP_HOME?"Not set."}

case "$1" in
  compile)
    $1 $APP_HOME
  ;;
  deploy)
    $1 $APP_HOME
  ;;
  start)
    $1 $APP_HOME
  ;;
  status)
    $1 $APP_HOME
  ;;
  start_mr)
    $1 $APP_HOME
  ;;
  stream)
     $1 $2 $3
  ;;
  proc)
    $1 $2 $3
  ;;
  promote)
    $1 $APP_HOME $3 $4
  ;;
  multiClientStream)
    $1 $2 $3 $4
  ;;


  *)
    echo "Usage: $0 {compile|deploy|start|status|start_mr|stream|multiClientStream|proc|promote}"
    exit 1
  ;;
esac

exit $?


