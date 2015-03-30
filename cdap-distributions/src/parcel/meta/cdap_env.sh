#!/bin/bash

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

export CDAP_HOME=${PARCELS_ROOT}/${PARCEL_DIRNAME}
export CDAP_AUTH_SERVER_HOME=${CDAP_HOME}/security
export CDAP_AUTH_SERVER_CONF_SCRIPT=${CDAP_AUTH_SERVER_HOME}/conf/auth-server-env.sh
export CDAP_KAFKA_SERVER_HOME=${CDAP_HOME}/kafka
export CDAP_KAFKA_SERVER_CONF_SCRIPT=${CDAP_KAFKA_SERVER_HOME}/conf/kafka-server-env.sh
export CDAP_MASTER_HOME=${CDAP_HOME}/master
export CDAP_MASTER_CONF_SCRIPT=${CDAP_MASTER_HOME}/conf/master-env.sh
export CDAP_ROUTER_HOME=${CDAP_HOME}/gateway
export CDAP_ROUTER_CONF_SCRIPT=${CDAP_ROUTER_HOME}/conf/router-env.sh
export CDAP_WEB_APP_HOME=${CDAP_HOME}/web-app
export CDAP_WEB_APP_CONF_SCRIPT=${CDAP_WEB_APP_HOME}/conf/web-app-env.sh
