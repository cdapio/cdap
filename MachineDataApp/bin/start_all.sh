#!/bin/bash


if [ $# -ne 2 ]; then
	echo "Required argument: <num_metrics> <rate in seconds>"
	exit
fi

./collect_cpu.sh $1 $2 &
./collect_mem.sh $1 $2 &
./collect_disk.sh $1 $2 &
