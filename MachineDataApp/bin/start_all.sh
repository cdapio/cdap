#!/bin/bash


if [ $# -ne 1 ]; then
	echo "Required argument: <num_metrics>"
	exit
fi

./collect_cpu.sh $1 &
./collect_mem.sh $1 &
./collect_disk.sh $1 &
