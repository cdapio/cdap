# Copyright 2012-2014 Continuuity, Inc.
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

# Set environment variables here.

# The java implementation to use.  Java 1.6 required.
# export JAVA_HOME=/usr/java/jdk1.6.0/

# The maximum amount of heap to use, in MB. Default is 1000.
# export HEAPSIZE=1000

# Extra Java runtime options.
# Below are what we set by default.  May only work with SUN JVM.
# For more on why as well as other possible settings,
# see http://wiki.apache.org/hadoop/PerformanceTuning
export OPTS="-XX:+UseConcMarkSweepGC"

# Uncomment below to enable java garbage collection logging in the .out file.
# export GC_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps"

# Uncomment below (along with above GC logging) to put GC information in its own logfile
# export USE_GC_LOGFILE=true

# Where log files are stored.  $CONTINUUITY_HOME/logs by default.
export LOG_DIR=/var/log/continuuity

# A string representing this instance of hbase. $USER by default.
export IDENT_STRING=$USER

# The scheduling priority for daemon processes.  See 'man nice'.
# export NICENESS=10

# The directory where pid files are stored. /tmp by default.
export PID_DIR=/var/continuuity/run

# Specifies the JAVA_HEAPMAX
export JAVA_HEAPMAX=-Xmx128m

# Main class to be invoked.
MAIN_CLASS=

# Arguments for main class.
#MAIN_CLASS_ARGS=""

# Reactor adds Hadoop and HBase libs to the classpath on startup.
# If the "hbase" command is on the PATH, this will be done automatically.
# Or uncomment the line below to point to the HBase installation directly.
# HBASE_HOME=

# Extra CLASSPATH
# EXTRA_CLASSPATH=""
