#!/usr/bin/env bash

# WARNING: Not Production quality code.
# Copyright (c) to Continuuity Inc. All rights reserved.

pwd=`dirname "${BASH_SOURCE-$0}"`

# Determine the Java command to use to start the JVM.
if [ -n "$JAVA_HOME" ] ; then
    if [ -x "$JAVA_HOME/jre/sh/java" ] ; then
        # IBM's JDK on AIX uses strange locations for the executables
        JAVA="$JAVA_HOME/jre/sh/java"
    else
        JAVA="$JAVA_HOME/bin/java"
    fi
    if [ ! -x "$JAVA" ] ; then
        die "ERROR: JAVA_HOME is set to an invalid directory: $JAVA_HOME

Please set the JAVA_HOME variable in your environment to match the
location of your Java installation."
    fi
else
    JAVA="java"
    which java >/dev/null 2>&1 || die "ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH.

Please set the JAVA_HOME variable in your environment to match the
location of your Java installation."
fi

if [[ "X${CONTINUUITY_HOME}" == "X" ]]; then
	echo "Please set CONTINUUITY_HOME"
	exit
fi

# Set gateway host and port.
if [[ "X${GATEWAY_HOSTNAME}" == "X" ]]; then
  GATEWAY_HOSTNAME="localhost"
fi

if [[ "X${GATEWAY_PORT}" == "X" ]]; then
  GATEWAY_PORT="10000"
fi

# Initialize variables
num_actions=$1
if [[ "X${1}" == "X" ]]; then
  num_actions=1000
else
  num_actions=$1
fi

action_id=${action_id:=100} # Initial action id (increments from here)
product_id=${product_id:=2000} # Initial product id (randomly increments from here)
product_id_max_increment=${product_id_max_increment:=200}
num_stores=${num_stores:=75} # Number of stores (products auto-bucketed to stores)
clusters=( "Sports" )
num_clusters=${#clusters[@]};

echo "Continuuity AppFabric Demo"

echo " > Populating clusters through ClusterWriter flow."
$CONTINUUITY_HOME/bin/stream-client send --stream clusters --body '1,"Sports",0.001' --host localhost 2>/dev/null
echo " > Clusters updated..."

# Generating actions.
for (( i=0; i<$num_actions; i++ )); do
  product_id=$(($product_id + ($RANDOM % $product_id_max_increment)))
  store_id=$(($product_id % $num_stores))
  date=$((($(date +%s)*1000) + ($RANDOM % 1000)))
  json="{\"@id\":\"$action_id\",\"product_id\":\"$product_id\",\"store_id\":\"$store_id\",\"category\":\"Sports\",\"actor_id\":\"301\",\"type\":\"yay-exp-action\",\"date\":\"$date\",\"@view\":\"SIMPLE\",\"score\":\"1.0\"}"
  CMD="curl http://${GATEWAY_HOSTNAME}:${GATEWAY_PORT}/stream/social-actions --request POST --data '${json}' 2>/dev/null"
  eval "$CMD"
  (( action_id ++ ))
  m=$(($i % 100))
  if [[ "X${m}" == "X0" ]]; then
    echo ""
    echo " > Querying results" 
    curl --request POST "http://${GATEWAY_HOSTNAME}:10010/procedure/LishApp/ClusterFeedQueryProvider/readactivity" --data "{'country':'US','clusterid':1,'limit':10}"
  fi
  if [[ "X${m}" == "X5" ]]; then
    echo ""
    echo " > Sending actions ..."
  fi
done
