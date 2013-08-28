#!/bin/bash
if [ "$#" -ne 1 ] ; then
     echo "Usage: $0 [hostname]" >&2
     exit 1
fi

while : 
do
   i=`curl -s -X POST http://$1:10010/procedure/WordCount/RetrieveCounts/getCount --data {"word":"today"}`
   echo "$i" 
done


