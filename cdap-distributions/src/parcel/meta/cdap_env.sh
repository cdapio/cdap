#!/bin/bash
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
