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
bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`
lib="$bin"/../lib
conf="$bin"/../conf
CDAP_CONF=${CDAP_CONF:-/etc/cdap/conf}

script=`basename $0`

# Resolve relative symlinks
while [ -h "$PRG" ] ; do
    ls=`ls -ld "$PRG"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '/.*' > /dev/null; then
        PRG="$link"
    else
        PRG=`dirname "$PRG"`"/$link"
    fi
done
SAVED="`pwd`"
cd "`dirname \"$PRG\"`/.."
APP_HOME="`pwd -P`"

# Where log files are stored.  $CDAP_HOME/logs by default.
export LOG_DIR=/var/log

# A string representing this instance of hbase. $USER by default.
export IDENT_STRING=$USER

# The directory where pid files are stored. /tmp by default.
export PID_DIR=/var/run

pid=$PID_DIR/tx-service-${IDENT_STRING}.pid

if [ ! -e "$pid" ] ; then
  touch "$pid"
fi


# In other environment, the jars are expected to be in <HOME>/lib directory.
# Load all the jar files. Not ideal, but we need to load only the things that
# is needed by this script.
if [ "$CLASSPATH" = "" ]; then
  CLASSPATH=${lib}/*
else
  CLASSPATH=$CLASSPATH:${lib}/*
fi

# Load the configuration too.
if [ -d "$CDAP_CONF" ]; then
  CLASSPATH=$CLASSPATH:"$CDAP_CONF"
elif [ -d "$conf" ]; then
  CLASSPATH=$CLASSPATH:"$conf"/
fi

# Set Log location
export LOG_PREFIX="tx-service-$IDENT_STRING-$HOSTNAME"
export LOGFILE=$LOG_PREFIX.log
loglog="${LOG_DIR}/${LOGFILE}"

if [ ! -e "$loglog" ] ; then
  touch "$loglog"
fi

# set the classpath to include hadoop and hbase dependencies
set_classpath()
{
  COMP_HOME=$1
  CCONF=$2
  if [ -n "$HBASE_HOME" ]; then
    HBASE_CP=`$HBASE_HOME/bin/hbase classpath`
  elif [ `which hbase` ]; then
    HBASE_CP=`hbase classpath`
  fi

  export HBASE_CP

  if [ -n "$HBASE_CP" ]; then
    CP=$COMP_HOME/lib/*:$HBASE_CP:$CCONF/:$COMP_HOME/conf/:$EXTRA_CLASSPATH
  else
    # assume Hadoop/HBase libs are included via EXTRA_CLASSPATH
    echo "WARN: could not find Hadoop and HBase libraries"
    CP=$COMP_HOME/lib/*:$CCONF/:$COMP_HOME/conf/:$EXTRA_CLASSPATH
  fi

  # Setup classpaths.
  if [ -n "$CLASSPATH" ]; then
    CLASSPATH=$CLASSPATH:$CP
  else
    CLASSPATH=$CP
  fi

  export CLASSPATH
}

stop() {
  if [ -f $pid ]; then
    pidToKill=`cat $pid`
    # kill -0 == see if the PID exists
    if kill -0 $pidToKill > /dev/null 2>&1; then
      echo -n stopping $command
      echo "`date` Terminating $command" >> $loglog
      kill $pidToKill > /dev/null 2>&1
      while kill -0 $pidToKill > /dev/null 2>&1;
      do
        echo -n "."
        sleep 1;
      done
      rm $pid
      echo
    else
      retval=$?
      echo nothing to stop because kill -0 of pid $pidToKill failed with status $retval
    fi
    rm -f $pid
  else
    echo nothing to stop because no pid file $pid
  fi
}


# set the classpaths
set_classpath

if [ $# -ne 1 ]; then
  echo "Usage: $0 {start|stop}"
  exit 1
fi

if [ "x$1" == "xstart" ]; then
  java -cp ${CLASSPATH} -Dscript=$script co.cask.cdap.data2.transaction.TransactionServiceMain "$@" <$loglog >>$loglog 2>&1 &
  echo $! >$pid
  exit 0
fi

if [ "x$1" == "xstop" ]; then
  stop
  exit 0
fi

if [ "x$1" == "xhelp" ]; then
  echo "Usage: $0 {start|stop}"
  exit 0
fi

# incorrect params
echo "Usage: $0 {start|stop}"
exit 1

