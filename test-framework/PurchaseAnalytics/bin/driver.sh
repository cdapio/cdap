#!/bin/bash
if [ "$#" -ne 2 ] ; then
     echo "Usage: $0 [hostname] [number of transactions]" >&2
     exit 1
fi


java -cp ../target/purchaseanalytics-1.0-jar-with-dependencies.jar com.continuuity.testsuite.purchaseanalytics.appdriver.PurchaseAnalyticsAppDriver $1 $2

