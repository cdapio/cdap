#!/usr/bin/env bash

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`
script=`basename $0`

function usage() {
  echo "Tool for sending data to the AccessLogApp"
  echo "Usage: $script [--gateway <hostname>]"
  echo ""
  echo "  Options"
  echo "    --gateway   Specifies the hostname the gateway is running on.(Default: localhost)"
  echo "    --help      This help message"
  echo ""
}

gateway="localhost"
stream="logEventStream"
  while [ $# -gt 0 ]
  do
    case "$1" in
      --gateway) shift; gateway="$1"; shift;;
      *)  usage; exit 1
     esac
  done
OLD_IFS=IFS
IFS=$'\n'
lines=`cat "$bin"/../resources/apache.accesslog`
for line in $lines
do
  curl -X POST -d "$line" http://$gateway:10000/v2/streams/$stream;
done
IFS=$OLD_IFS
