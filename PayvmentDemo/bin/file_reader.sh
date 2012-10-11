#!/bin/bash

# Require only one argument (the file to read)
if [ $# -ne 1 ]; then
	echo "Required argument: <filename>"
	exit
fi

filename=$1

# Ensure file exists
if [ ! -f $filename ]; then
	echo "File does not exist: $filename"
	exit
fi

while read line; do
	firstChar=${line:0:1}
	if [[ $firstChar == "" ]] || [[ $firstChar == "#" ]]; then
		# empty line or comment, skip
		echo -n
	else
		echo $line
	fi
done < $filename
