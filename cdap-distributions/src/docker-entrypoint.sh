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

set -e

export PATH=${PATH}:/opt/cdap/sdk/bin

# Add cdap sandbox start as command if needed
if [ "${1:0:1}" = '-' ]; then
  set -- cdap sandbox start --foreground "$@"
fi

# Drop root privileges if we are running cdap script
# allow the container to be started with `--user`
if [ "${1}" = 'cdap' -a "$(id -u)" = '0' ]; then
  # Change the ownership of /opt/cdap/sdk to cdap
  chown -R cdap:cdap /opt/cdap/sdk
  set -- gosu cdap "$@"
fi

# As argument is not related to cdap,
# then assume that user wants to run his own process,
# for example a `bash` shell to explore this image
exec "$@"
