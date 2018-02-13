#!/bin/bash
#
# Copyright © 2016 Cask Data, Inc.
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

# Shamelessly "borrowed" from https://github.com/docker-library/elasticsearch :-)

# for dev use. Cleanup old running apps and zookeeper dir
if [ "$CDAP_ROLE" == "master" ]; then
  for app in `yarn application -list | awk '$6 == "RUNNING" { print $1 }'`; do yarn application -kill "$app";  done
  echo "rmr /cdap" | zookeeper-client -server $HADOOP_HOST
fi

set -e

export PATH=${PATH}:/opt/cdap/$CDAP_COMPONENT/bin

# Add cdap $CDAP_ROLE start as command if needed
if [ "${1:0:1}" = '-' ]; then
  set -- service cdap-$CDAP_ROLE start "$@"
fi

# Drop root privileges if we are running cdap script
# allow the container to be started with `--user`
if [ "${1}" = 'cdap' -a "$(id -u)" = '0' ]; then
  # Change the ownership of /opt/cdap/sandbox to cdap
  chown -R cdap:cdap /opt/cdap/$CDAP_COMPONENT
  set -- gosu cdap "$@"
fi

# temp workaround until we get the --foreground working
#service cdap-$CDAP_ROLE start

# As argument is not related to cdap,
# then assume that user wants to run his own process,
# for example a `bash` shell to explore this image
echo "$@"
exec "$@"

# temp hack to keep the container alive.
#while true; do
#sleep 1
#done
