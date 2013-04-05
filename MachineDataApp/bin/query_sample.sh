#!/bin/bash

echo "Check for status..."
curl http://localhost:10005/monitor/MachineDataApp/MachineDataFlow/status -X GET
echo $'\n'
curl -X POST http://localhost:10010/procedure/MachineDataApp/MachineDataProcedure/echo --data [] 
echo $'\n'

echo $'\n'"Getting CPU metrics..."$'\n'
curl -X POST http://localhost:10010/procedure/MachineDataApp/MachineDataProcedure/getLastHour --data "{"hostname":"$HOSTNAME","type":"cpu"}" > cpu.json
echo $'\n'"Getting Memory metrics..."$'\n'
curl -X POST http://localhost:10010/procedure/MachineDataApp/MachineDataProcedure/getLastHour --data "{"hostname":"$HOSTNAME","type":"memory"}" > memory.json
echo $'\n'"Getting disk metrics..."$'\n'
curl -X POST http://localhost:10010/procedure/MachineDataApp/MachineDataProcedure/getLastHour --data "{"hostname":"$HOSTNAME","type":"disk"}" > disk.json
