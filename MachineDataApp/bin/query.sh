#!/bin/bash

#{"hostname":"ALEXMAC.local", "timestamp_from":"13644062480000", "timestamp_to":"13646063660000"}

curl -v  http://localhost:10005/monitor/MachineDataApp/MachineDataFlow/status -X GET

curl -v -X POST http://localhost:10010/query/MachineDataProcedure/getLastHour --data {"hostname":"ALEXMAC.local", "type":"cpu"}
