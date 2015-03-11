#!/bin/bash

#send data
curl -v http://127.0.0.1:10000/v2/streams/purchaseStream -X POST -d 'edwin bought 1 apple for $1'
curl -v http://127.0.0.1:10000/v2/streams/purchaseStream -X POST -d 'edwin bought 1 apple for $1'
curl -v http://127.0.0.1:10000/v2/streams/purchaseStream -X POST -d 'edwin bought 1 apple for $1'
curl -v http://127.0.0.1:10000/v2/streams/purchaseStream -X POST -d 'edwin bought 1 apple for $1'
curl -v http://127.0.0.1:10000/v2/streams/purchaseStream -X POST -d 'edwin bought 1 apple for $1'
curl -v http://127.0.0.1:10000/v2/streams/purchaseStream -X POST -d 'edwin bought 1 apple for $1'

#start flow
curl -v http://127.0.0.1:10000/v2/apps/PurchaseHistory/flows/PurchaseFlow/start -X POST

#Start services
curl -v http://127.0.0.1:10000/v2/apps/PurchaseHistory/services/CatalogLookup/start -X POST
curl -v http://127.0.0.1:10000/v2/apps/PurchaseHistory/services/UserProfileService/start -X POST

#Start workflow which starts MR
curl -v http://127.0.0.1:10000/v2/apps/PurchaseHistory/workflows/PurchaseHistoryWorkflow/start -X POST