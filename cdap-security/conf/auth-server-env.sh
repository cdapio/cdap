#!/usr/bin/env bash
#
# Copyright © 2014-2016 Cask Data, Inc.
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

# Main class to be invoked.
declare -r MAIN_CLASS=co.cask.cdap.security.runtime.AuthenticationServerMain

# Arguments for main class.
declare -r MAIN_CLASS_ARGS=

# Add Hadoop HDFS classpath
EXTRA_CLASSPATH="$HBASE_HOME/conf/"

# Java Heap settings
declare -r JAVA_HEAP_VAR=AUTH_JAVA_HEAPMAX
AUTH_JAVA_HEAPMAX=${AUTH_JAVA_HEAPMAX:--Xmx1024m}
