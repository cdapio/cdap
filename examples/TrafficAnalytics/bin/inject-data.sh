#!/usr/bin/env bash

#
# Copyright 2014 Cask, Inc.
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
  echo "    --host      Specifies the host that CDAP is running on. (Default: localhost)"
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
