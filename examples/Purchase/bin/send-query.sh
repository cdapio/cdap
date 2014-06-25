#!/usr/bin/env bash

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`
script=`basename $0`
user=$USER
epoch=`date +%s`

function usage() {
  echo "Querying tool for the Purchase application."
  echo "Usage: $script --query \"<sql query>\" [--gateway <hostname>]"
  echo ""
  echo "  Options"
  echo "    --query     Specifies the query to be executed."
  echo "    --gateway   Specifies the hostname the gateway is running on.(Default: localhost)"
  echo "    --help      This help message"
  echo ""
}

function query_action() {
  local q=$1; shift;
  local gateway=$1; shift;

  # send query and parse response for status and handle
  response=`curl -sL -w "%{http_code}\\n" -X POST http://localhost:10000/v2/data/queries -d \{\"query\":\""$q"\"\}`
  if [[ ! $response =~ 200$ ]]; then
    echo "Submit got response code $response. Error."
    exit 1;
  fi
  handle=${response/#\{\"handle\"\:\"/}
  handle=${handle/%\"\}200/}
  echo "Query handle is $handle."

  # wait for query completion
  status="UNKNOWN"
  while [ "x$status" != "xFINISHED" ]; do
    sleep 1;
    response=`curl -sL -w "%{http_code}\\n" -X GET http://localhost:10000/v2/data/queries/$handle/status`
    if [[ ! $response =~ 200$ ]]; then
      echo "Status got response code $response. Error."
      exit 1;
    fi
    status=${response/#\{\"status\"\:\"/}
    status=${status/%\",*/}
    # echo status is $status
    if [ "x$status" == "xCANCELED" ] || [ "x$status" == "xCLOSED" ] || [ "x$status" == "xERROR" ]; then
      echo "Query status is $status. Error."
      exit 1;
    fi
  done

  # retrieve results
  noresults=true;
  while true; do
    response=`curl -sL -w "%{http_code}\\n" -X POST http://localhost:10000/v2/data/queries/$handle/next -d '{"size":1}'`
    if [[ ! $response =~ 200$ ]]; then
      echo "Next call got response code $response. Error."
      exit 1;
    fi
    result=${response/%200/}
    if [[ ! $result =~ columns ]]; then
      if $noresults; then
        echo "No results. ";
      fi
      break;
    fi
    result=${result/#[\{\"columns\":/}
    result=${result/%\}]/}
    echo "$result";
    noresults=false;
  done

  # close the query
  curl -sL -X DELETE http://localhost:10000/v2/data/queries/$handle
}

gateway="localhost"
query=
while [ $# -gt 0 ]; do
  case "$1" in
    --query) shift; query="$1"; shift;;
    --gateway) shift; gateway="$1"; shift;;
    *)  usage; exit 1
  esac
done


if [ "x$query" == "x" ]; then
  usage
  echo "Query not specified."
  exit 1
fi

query_action "$query" $gateway
