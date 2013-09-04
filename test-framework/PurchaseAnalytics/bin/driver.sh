#!/bin/bash
if [ "$#" -ne 2 ] ; then
     echo "Usage: $0 [hostname] [number of transactions]" >&2
     exit 1
fi

: ${REACTOR_HOME?"Not set."}

echo "Deploying application on $1."
#$REACTOR_HOME/bin/reactor-client deploy --archive ../target/purchaseanalytics-1.0-jar-with-dependencies.jar  --host $1

if [ $? != 0 ]; then
  echo "Error deploying app."
  exit 1
fi

echo "Starting driver."
java -cp ../target/purchaseanalytics-1.0-jar-with-dependencies.jar com.continuuity.testsuite.purchaseanalytics.appdriver.PurchaseAnalyticsAppDriver $1 $2
echo "Driver Started."
