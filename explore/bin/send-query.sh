#!/usr/bin/env bash

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`
script=`basename $0`
user=$USER
epoch=`date +%s`

auth_token=
auth_file="$HOME/.continuuity.accesstoken"

function get_auth_token() {
  if [ -f $auth_file ]; then
    auth_token=`cat $auth_file`
  fi
}

function usage() {
  echo "Querying tool for exploring Datasets with SQL."
  echo "Usage: $script --query \"<sql query>\" [--host <hostname>]"
  echo ""
  echo "  Options"
  echo "    --query     Specifies the query to be executed."
  echo "    --host      Specifies the host that Reactor is running on. (Default: localhost)"
  echo "    --help      This help message"
  echo "  If reactor requires an access token, it needs to be in $auth_file"
  echo ""
}

function query_action() {
  local q=$1; shift;
  local host=$1; shift;

  # send query and parse response for status and handle
  response=`curl -sL -w "#%{http_code}\\n" -H "Authorization: Bearer $auth_token" -X POST   \
    http://$host:10000/v2/data/queries -d \{\"query\":\""$q"\"\}`

  if [ $? != "0" ]; then
    echo "Cannot connect to $host"
    exit 1;
  fi

  if [[ ! $response =~ 200$ ]]; then
    if [[ $response =~ 401$ ]]; then
      if [ "x$auth_token" == "x" ]; then
        echo "No access token provided"
      else
        echo "Invalid access token"
      fi
      exit 1;
    fi
    echo "ERROR:" `echo $response | awk -F"#" ' { print $1 } '`
    exit 1;
  fi

  handle=${response/#\{\"handle\"\:\"/}
  handle=${handle/%\"\}#200/}
  echo "Query handle is $handle."

  # wait for query completion
  status="UNKNOWN"
  while [ "x$status" != "xFINISHED" ]; do
    sleep 1;
    response=`curl -sL -w "%{http_code}\\n" -H "Authorization: Bearer $auth_token" -X GET http://$host:10000/v2/data/queries/$handle/status`
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
    response=`curl -sL -w "%{http_code}\\n" -H "Authorization: Bearer $auth_token" -X POST http://$host:10000/v2/data/queries/$handle/next -d '{"size":1}'`
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
  curl -sL -H "Authorization: Bearer $auth_token" -X DELETE http://$host:10000/v2/data/queries/$handle
}

host="localhost"
query=
while [ $# -gt 0 ]; do
  case "$1" in
    --query) shift; query="$1"; shift;;
    --host) shift; host="$1"; shift;;
    *)  usage; exit 1
  esac
done


if [ "x$query" == "x" ]; then
  usage
  echo "Query not specified."
  exit 1
fi

# read the access token
get_auth_token

query_action "$query" $host
