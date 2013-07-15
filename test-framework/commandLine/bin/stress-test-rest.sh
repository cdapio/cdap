#!/bin/sh

: ${APP_HOME?"not set."}

if [ $# -lt 1 ]
then
    echo "usage test-stress-rest.sh <number of iterations>"
    exit 1
fi

echo "Starting Count..."
$APP_HOME/bin/reactor-client start --archive $APP_HOME/examples/WordCount/WordCount.jar
sleep 3

echo "Start Stress..."
for (( i=0; i<$1; i++ )); do
msg=`date`;
curl -q -d "today $msg" http://localhost:10000/stream/wordStream;
echo "sending: $msg"
done
