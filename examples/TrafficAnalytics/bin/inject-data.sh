#!/usr/bin/env bash

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`
script=`basename $0`

auth_token=
auth_file="$HOME/.continuuity.accesstoken"

function get_auth_token() {
  if [ -f $auth_file ]; then
    auth_token=`cat $auth_file`
  fi
}

function usage() {
  echo "Tool for sending data to the TrafficAnalytics app"
  echo "Usage: $script [--host <host>]"
  echo ""
  echo "  Options"
  echo "    --host      Specifies the host that Reactor is running on. (Default: localhost)"
  echo "    --help      This help message"
  echo ""
}

gateway="localhost"
while [ $# -gt 0 ]
do
  case "$1" in
    --host) shift; gateway="$1"; shift;;
    *)  usage; exit 1
   esac
done

#  get the access token
get_auth_token

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
  status=`curl -qSfsw "%{http_code}\\n" -H "Authorization: Bearer $auth_token" -X POST -d "$newLine" http://$gateway:10000/v2/streams/$stream`
done
IFS=$OLD_IFS
