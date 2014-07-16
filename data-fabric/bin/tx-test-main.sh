
#!/bin/sh

# Attempt to set APP_HOME
# Resolve links: $0 may be a link
PRG="$0"
bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`
lib="$bin"/../lib
conf="$bin"/../conf
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

# Where log files are stored.  $CONTINUUITY_HOME/logs by default.
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
if [ -d "$conf" ]; then
  CLASSPATH=$CLASSPATH:"$conf"/
fi

# Load all necessary configs
CLASSPATH="$CLASSPATH:/etc/continuuity/conf/:/etc/hadoop/conf/:/usr/lib/hadoop/*:/usr/lib/hadoop-hdfs/*:/usr/lib/hbase/lib/*:/usr/lib/hadoop-yarn/*:/opt/continuuity/hbase-compat-0.96/lib/*:/etc/zookeeper/conf/*"

# Set Log location
export LOG_PREFIX="tx-service-$IDENT_STRING-$HOSTNAME"
export LOGFILE=$LOG_PREFIX.log
loglog="${LOG_DIR}/${LOGFILE}"

if [ ! -e "$loglog" ] ; then
  touch "$loglog"
fi


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

if [ $# -ne 1 ]; then
  echo "Usage: $0 {start|stop}"
  exit 1
fi

if [ "x$1" == "xstart" ]; then
  java -cp ${CLASSPATH} -Dscript=$script com.continuuity.data2.transaction.TransactionServiceMain "$@" <$loglog >>$loglog 2>&1 &
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
exit 1

