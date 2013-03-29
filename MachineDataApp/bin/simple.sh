#!/bin/bash 
curl  http://localhost:10000/stream/cpuStatsStream --request PUT --data "1364509103, 10, hostname" -v
