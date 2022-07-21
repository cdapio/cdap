#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# echo commands to the terminal output
set -ex

# Check whether there is a passwd entry for the container UID
myuid=$(id -u)
mygid=$(id -g)
# turn off -e for getent because it will return error code in anonymous uid case
set +e
uidentry=$(getent passwd $myuid)
set -e

# If there is no passwd entry for the container UID, attempt to create one
if [ -z "$uidentry" ] ; then
    if [ -w /etc/passwd ] ; then
	echo "$myuid:x:$myuid:$mygid:${SPARK_USER_NAME:-anonymous uid}:$SPARK_HOME:/bin/false" >> /etc/passwd
    else
	echo "Container ENTRYPOINT failed to add passwd entry for anonymous UID"
    fi
fi

SPARK_CLASSPATH="$SPARK_CLASSPATH:${SPARK_HOME}/jars/*"
env | grep SPARK_JAVA_OPT_ | sort -t_ -k4 -n | sed 's/[^=]*=\(.*\)/\1/g' > /tmp/java_opts.txt
readarray -t SPARK_EXECUTOR_JAVA_OPTS < /tmp/java_opts.txt

if [ -n "$SPARK_EXTRA_CLASSPATH" ]; then
  SPARK_CLASSPATH="$SPARK_CLASSPATH:$SPARK_EXTRA_CLASSPATH"
fi

if ! [ -z ${PYSPARK_PYTHON+x} ]; then
    export PYSPARK_PYTHON
fi
if ! [ -z ${PYSPARK_DRIVER_PYTHON+x} ]; then
    export PYSPARK_DRIVER_PYTHON
fi

# If HADOOP_HOME is set and SPARK_DIST_CLASSPATH is not set, set it here so Hadoop jars are available to the executor.
# It does not set SPARK_DIST_CLASSPATH if already set, to avoid overriding customizations of this value from elsewhere e.g. Docker/K8s.
if [ -n "${HADOOP_HOME}"  ] && [ -z "${SPARK_DIST_CLASSPATH}"  ]; then
  export SPARK_DIST_CLASSPATH="$($HADOOP_HOME/bin/hadoop classpath)"
fi

if ! [ -z ${HADOOP_CONF_DIR+x} ]; then
  SPARK_CLASSPATH="$HADOOP_CONF_DIR:$SPARK_CLASSPATH";
fi

if ! [ -z ${SPARK_CONF_DIR+x} ]; then
  SPARK_CLASSPATH="$SPARK_CONF_DIR:$SPARK_CLASSPATH";
elif ! [ -z ${SPARK_HOME+x} ]; then
  SPARK_CLASSPATH="$SPARK_HOME/conf:$SPARK_CLASSPATH";
fi

# add cdap-spark-core to classpath
# add it to SPARK_DIST_CLASSPATH because that is the only way to add it to both
# driver and executor classpath
# spark.driver.extraClassPath does not get added because
# the command builder looks specifically for the SparkSubmit class,
# whereas we run CDAP's SparkContainerLauncher class
# SPARK_EXTRA_CLASSPATH does not work either for the same reason
# that the ContainerLauncher is not a known Spark class
# and gets ignored by org.apache.spark.launcher.Main
# /etc/cdap/localizefiles will contain the logback jar and must come first in the classpath
CDAP_SPARK_CORE_LIBS=`find /opt/cdap/cdap-spark-core/lib | sort | tr '\n' ':'`
export CDAP_SPARK_CLASSPATH="/etc/cdap/localizefiles/logback.xml.jar:/opt/cdap/cdap-spark-core/cdap-spark-core.jar:$CDAP_SPARK_CORE_LIBS:/etc/cdap/conf"

case "$1" in
  driver)
    shift 1
    CMD=(
      # launch SparkSubmit with the CDAP launcher, which will
      # setup CDAP classloading and class rewrite
      ${JAVA_HOME}/bin/java
      # CDAP classpath must come before spark classpath, otherwise
      # recursive logging and task deserialization errors occur
      -cp "$CDAP_SPARK_CLASSPATH:$SPARK_CLASSPATH"
      io.cdap.cdap.app.runtime.spark.distributed.k8s.SparkContainerDriverLauncher
      --delegate-class org.apache.spark.deploy.SparkSubmit
      --conf "spark.driver.bindAddress=$SPARK_DRIVER_BIND_ADDRESS"
      --artifact-fetcher-uri $ARTIFACT_FECTHER_URI
      --deploy-mode client
      "$@"
    )
    ;;
  executor)
    shift 1
    CMD=(
      ${JAVA_HOME}/bin/java
      "${SPARK_EXECUTOR_JAVA_OPTS[@]}"
      -Xms$SPARK_EXECUTOR_MEMORY
      -Xmx$SPARK_EXECUTOR_MEMORY
      # CDAP classpath must come before spark classpath, otherwise
      # recursive logging and task deserialization errors occur
      -cp "$CDAP_SPARK_CLASSPATH:$SPARK_CLASSPATH"
      # replace Spark executor class with the ContainerLauncher
      # configured to launch the executor after performing classloader setup
      io.cdap.cdap.app.runtime.spark.distributed.k8s.SparkContainerExecutorLauncher
      --delegate-class org.apache.spark.executor.CoarseGrainedExecutorBackend
      --driver-url $SPARK_DRIVER_URL
      --executor-id $SPARK_EXECUTOR_ID
      --cores $SPARK_EXECUTOR_CORES
      --app-id $SPARK_APPLICATION_ID
      --hostname $SPARK_EXECUTOR_POD_IP
      --resourceProfileId $SPARK_RESOURCE_PROFILE_ID
      --artifact-fetcher-port $ARTIFACT_FECTHER_PORT
    )
    ;;

  *)
    echo "Non-spark-on-k8s command provided, proceeding in pass-through mode..."
    CMD=("$@")
    ;;
esac

# Execute the container CMD under tini for better hygiene
exec /usr/bin/tini -s -- "${CMD[@]}"
