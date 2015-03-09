#!/usr/bin/env bash

#
# Copyright © 2015 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`
script=`basename $0`

auth_token=
auth_file="$HOME/.cdap.accesstoken"

verbose=false

function get_auth_token() {
  if [ -f $auth_file ]; then
    auth_token=`cat $auth_file`
  fi
}

function usage() {
  echo "Tool for sending random events to the activity flow."
  echo "Usage: $script [ --host <hostname> ] [ --file <file> ] --league <league> --season <season>"
  echo ""
  echo "  Options"
  echo "    --host      Specifies the host that CDAP is running on. (Default: localhost)"
  echo "    --file      The file to upload. Defaults to resources/<league>-<season>.csv"
  echo "    --league    The league the results are from"
  echo "    --season    The season the results are for"
  echo "    --verbose   Print more verbose information"
  echo "    --help      This help message"
  echo ""
}

gateway="localhost"
file=
league=
season=
while [ $# -gt 0 ]
do
  case "$1" in
    --host) shift; gateway="$1"; shift;;
    --file) shift; file="$1"; shift;;
    --league) shift; league="$1"; shift;;
    --season) shift; season="$1"; shift;;
    --verbose) shift; verbose=true;;
    *)  usage; exit 1
   esac
done

if [ "x$league" = "x" ] || [ "x$season" = "x" ]; then
  usage; exit 1;
fi

if [ "x$file" = "x" ]; then
  res=`cd "$bin/../resources"; pwd`
  file=$res/$league-$season.csv
fi

if ! [ -f "$file" ]; then
  echo "File $file does not exist or is not a regular file."
  exit 1;
fi

#  get the access token
get_auth_token

if [ $verbose = "true" ]; then
  echo "Uploading $file for the $season season of the $league."
fi

status=`curl -qSfsw "%{http_code}\\n" -H "Authorization: Bearer $auth_token" -X PUT --data-binary @$file \
    http://$gateway:10000/v3/namespaces/default/apps/SportResults/services/UploadService/methods/leagues/$league/seasons/$season`
if [ $status -ne 200 ]; then
  echo "Failed to send data."
  if [ $status == 401 ]; then
    if [ "x$auth_token" == "x" ]; then
      echo "No access token provided"
    else
      echo "Invalid access token"
    fi
  fi
  exit 1;
fi

