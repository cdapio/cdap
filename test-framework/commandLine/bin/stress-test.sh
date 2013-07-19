#!/bin/sh

if [ $# -lt 2 ]
then
    echo "Usage: stress-test.sh <num_iteration>  <APP_HOME>"
    exit 1
fi

APP_HOME=$2

echo "Starting wordCount stress run..."
$APP_HOME/bin/reactor-client start --archive $APP_HOME/examples/WordCount/WordCount.jar
sleep 3

echo "Start Stress..."
for (( i=0; i<$1; i++ )); do
msg=`date`;
curl -q -d "today $msg" http://localhost:10000/stream/wordStream;
echo "sending: $msg"
done
echo "Stress run completed."
