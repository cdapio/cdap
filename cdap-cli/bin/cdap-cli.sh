#!/usr/bin/env bash

#
# Copyright © 2014 Cask Data, Inc.
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

# Attempt to set APP_HOME
# Resolve links: $0 may be a link
PRG="$0"

# Need this for relative symlinks.
while [ -h "$PRG" ] ; do
    ls=`ls -ld "$PRG"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '/.*' > /dev/null; then
        PRG="$link"
    else
        PRG=`dirname "$PRG"`"/$link"
    fi
done

# Determine the Java command to use to start the JVM.
if [ -n "$JAVA_HOME" ] ; then
    if [ -x "$JAVA_HOME/jre/sh/java" ] ; then
        # IBM's JDK on AIX uses strange locations for the executables
        JAVACMD="$JAVA_HOME/jre/sh/java"
    else
        JAVACMD="$JAVA_HOME/bin/java"
    fi
    if [ ! -x "$JAVACMD" ] ; then
        echo "ERROR: JAVA_HOME is set to an invalid directory: $JAVA_HOME
Please set the JAVA_HOME variable in your environment to match the
location of your Java installation."
        exit 1
    fi
else
    JAVACMD="java"
    which java >/dev/null 2>&1 || die "ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH.
Please set the JAVA_HOME variable in your environment to match the
location of your Java installation."
fi

# java version check
JAVA_VERSION=`$JAVACMD -version 2>&1 | grep "java version" | awk '{print $3}' | awk -F '.' '{print $2}'`
if [ $JAVA_VERSION -ne 7 ] && [ $JAVA_VERSION -ne 8 ]; then
  echo "ERROR: Java version not supported. Please install Java 7 or 8 - other versions of Java are not supported."
  exit -1
fi

bin=`dirname "${PRG}"`
bin=`cd "$bin"; pwd`
lib="$bin"/../lib
libexec="$bin"/../libexec
conf="$bin"/../conf
CDAP_CONF=${CDAP_CONF:-/etc/cdap/conf}
script=`basename $0`

CLI_CP=${libexec}/co.cask.cdap.cdap-cli-@@project.version@@.jar:${lib}/co.cask.cdap.cdap-cli-@@project.version@@.jar

if [ "$CLASSPATH" = "" ]; then
  CLASSPATH=$CLI_CP
else
  CLASSPATH=$CLASSPATH:$CLI_CP
fi

# Load the configuration too.
if [ -d "$CDAP_CONF" ]; then
  CLASSPATH=$CLASSPATH:"$CDAP_CONF"
elif [ -d "$conf" ]; then
  CLASSPATH=$CLASSPATH:"$conf"/
fi

CDAP_HOME="$bin"/..
export CDAP_HOME
$JAVACMD $JAVA_OPTS -cp ${CLASSPATH} -Dscript=$script co.cask.cdap.cli.CLIMain "$@"
