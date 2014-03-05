#!/usr/bin/env bash

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`
script=`basename $0`

function usage() {
  echo "Tool for sending data to the TrafficAnalytics app"
  echo "Usage: $script [--gateway <hostname>]"
  echo ""
  echo "  Options"
  echo "    --gateway   Specifies the hostname the gateway is running on.(Default: localhost:10000)"
  echo "    --help      This help message"
  echo ""
}

gateway="localhost"
  while [ $# -gt 0 ]
  do
    case "$1" in
      --gateway) shift; gateway="$1"; shift;;
      *)  usage; exit 1
     esac
  done

getDateInADay() {
  ((r=$RANDOM % 86400 + 1 ))
  ((time=${1}-$r))
  date=`date -r $time +'%d/%b/%Y:%H:%M:%S'`
  printf $date
}

now=`date +"%s"`
#echo $now
#echo `date +"%d/%b/%Y:%H:%M:%S"`

OLD_IFS=IFS
IFS=$'\n'
lines=`cat "$bin"/../resources/apache.accesslog`

stream="logEventStream"
echo 'inject log data:'
for line in $lines
do
  date=$(getDateInADay $now)
  newLine=`echo $line | sed -E "s,([0-9]{2})/([A-Za-z]{3})/([0-9]{4}):([0-9]{2}):([0-9]{2}):([0-9]{2}),$date,g" ` 
  echo $newLine
  curl -X POST -d "$newLine" http://$gateway:10000/v2/streams/$stream;
done
IFS=$OLD_IFS
