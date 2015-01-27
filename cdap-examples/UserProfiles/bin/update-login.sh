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

seconds=100
sleep=0.01
verbose=false

function get_auth_token() {
  if [ -f $auth_file ]; then
    auth_token=`cat $auth_file`
  fi
}

function usage() {
  echo "Tool for updating user's last login time randomly."
  echo "Usage: $script [--host <hostname>]"
  echo ""
  echo "  Options"
  echo "    --host      Specifies the host that CDAP is running on. (Default: localhost)"
  echo "    --seconds   How many seconds to run"
  echo "    --delay     How many seconds to sleep between each call (e.g., 0.1 for 100 ms)"
  echo "    --verbose   Print some information"
  echo "    --help      This help message"
  echo ""
}

gateway="localhost"
stream="events"
while [ $# -gt 0 ]
do
  case "$1" in
    --host) shift; gateway="$1"; shift;;
    --seconds) shift; seconds="$1"; shift;;
    --delay) shift; sleep="$1"; shift;;
    --verbose) shift; verbose=true;;
    *)  usage; exit 1
   esac
done

#  get the access token
get_auth_token

OLD_IFS=IFS
IFS=$'\n'
userids=(`awk -F\" '{ print $4 }' "$bin"/../resources/users.txt`)
numusers=${#userids[@]}

starttime=`date "+%s"`
endtime=$(($starttime + $seconds))

while [ $(date "+%s") -lt $endtime ]; do
  index=$(($RANDOM % $numusers))
  userid=${userids[$index]}
  time=`date "+%s"`
  if [ $verbose == "true" ]; then
    echo Updating last login time for user id $userid to $time
  fi
  status=`curl -qSfsw "%{http_code}\\n" -H "Authorization: Bearer $auth_token" -X PUT -d$time \
    http://$gateway:10000/v2/apps/UserProfiles/services/UserProfileService/methods/profiles/$userid/lastLogin`
  if [ $status -ne 200 ]; then
    echo "Failed to send data."
    if [ $status == 401 ]; then
      if [ "x$auth_token" == "x" ]; then
        echo "No access token provided"
      else
        echo "Invalid access token"
      fi
    fi
    echo "Exiting program..."
    exit 1;
  fi
  sleep $sleep
done

IFS=$OLD_IFS
