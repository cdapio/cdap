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

# Example environment variables. Please refer to the CDAP Administration Manual
# for more information.

# If running CDAP on HDP 2.2+ the full HDP version string including iteration
#   number must be passed in as an option.
# export OPTS="${OPTS} -Dhdp.version=2.2.6.0-2800"

# Override detected SPARK_HOME for Spark support
# export SPARK_HOME="/usr/lib/spark"

# Adds Hadoop and HBase libs to the classpath on startup.
# If the "hbase" command is on the PATH, this will be done automatically.
# Or uncomment the line below to point to the HBase installation directly.
# HBASE_HOME=

# Extra CLASSPATH locations to add to CDAP Java processes
# EXTRA_CLASSPATH=""

# LOCAL_DIR sets the JVM -Duser.dir property of the CDAP processes, and provides a
# local working directory for application jars during startup if needed
LOCAL_DIR="/var/tmp/cdap"

# TEMP_DIR sets the JVM -Djava.io.tmpdir property of the CDAP processes, and provides
# temporary storage for Apache Twill
TEMP_DIR="/tmp"

# Service-specific Java heap settings (overrides defaults)
# export AUTH_JAVA_HEAPMAX="-Xmx1024m"
# export KAFKA_JAVA_HEAPMAX="-Xmx1024m"
# export MASTER_JAVA_HEAPMAX="-Xmx1024m"
# export ROUTER_JAVA_HEAPMAX="-Xmx1024m"

# Service-specific Java options (added to OPTS, before other options)
# export AUTH_JAVA_OPTS=""
# export KAFKA_JAVA_OPTS=""
# export MASTER_JAVA_OPTS=""
# export ROUTER_JAVA_OPTS=""
