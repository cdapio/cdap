# checks if there exists a PID that is already running. return 0 idempotently
check_before_start()
{
  if [ ! -d "$PID_DIR" ]; then
    mkdir -p "$PID_DIR"
  fi
  if [ -f $pid ]; then
    if kill -0 `cat $pid` > /dev/null 2>&1; then
      #echo "$APP $SERVICE running as process `cat $pid`. Stop it first."
      echo "$APP running as process `cat $pid`. Stop it first."
      exit 0
    fi
  fi
}

# Rotates the basic start/stop logs
rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
    num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
    while [ $num -gt 1 ]; do
        prev=`expr $num - 1`
        [ -f "$log.$prev" ] && mv -f "$log.$prev" "$log.$num"
        num=$prev
    done
    mv -f "$log" "$log.$num";
    fi
}

# Attempts to find JAVA in few ways.
set_java ()
{
  # Determine the Java command to use to start the JVM.
  if [ -n "$JAVA_HOME" ] ; then
    if [ -x "$JAVA_HOME/jre/sh/java" ] ; then
        # IBM's JDK on AIX uses strange locations for the executables
        export JAVA="$JAVA_HOME/jre/sh/java"
    else
        export JAVA="$JAVA_HOME/bin/java"
    fi
    if [ ! -x "$JAVA" ] ; then
        echo "ERROR: JAVA_HOME is set to an invalid directory: $JAVA_HOME

Please set the JAVA_HOME variable in your environment to match the
location of your Java installation." >&2
        exit 1
    fi
else
    export JAVA="java"
    which java >/dev/null 2>&1 || { echo "ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH.
Please set the JAVA_HOME variable in your environment to match the
location of your Java installation." >&2 ; exit 1; }
fi
}

# check and set classpath if in development enviroment
check_and_set_classpath_for_dev_environment ()
{
  APP_HOME=$1

  # Detect if we are in development.
  in_dev_env=false
  if [ -n "$IN_DEV_ENVIRONMENT" ]; then
    in_dev_env=true
  fi

  # for developers only, add flow and flow related stuff to class path.
  if $in_dev_env; then
    echo "Constructing classpath for development environment ..."
    if [ -f "$APP_HOME/build/generated-classpath" ]; then
      CLASSPATH=${CLASSPATH}:`cat $APP_HOME/build/generated-classpath`
    fi
    if [ -d "$APP_HOME/build/classes" ]; then
      CLASSPATH=${CLASSPATH}:$APP_HOME/build/classes/main
      CLASSPATH=${CLASSPATH}:$APP_HOME/conf/*
    fi
    if [ -d "$APP_HOME/../data-fabric/build/classes" ]; then
      CLASSPATH=${CLASSPATH}:$APP_HOME/../data-fabric/build/classes/main
    fi
    if [ -d "$APP_HOME/../common/build/classes" ]; then
      CLASSPATH=${CLASSPATH}:$APP_HOME/../common/build/classes/main
    fi
    if [ -d "$APP_HOME/../gateway/build/classes" ]; then
      CLASSPATH=${CLASSPATH}:$APP_HOME/../common/build/classes/main
    fi
    export CLASSPATH
  fi
}

#export LOG_PREFIX=$SERVICE-$IDENT_STRING-$HOSTNAME
export LOG_PREFIX=$APP-$IDENT_STRING-$HOSTNAME
export LOGFILE=$LOG_PREFIX.log
loglog="${LOG_DIR}/${LOGFILE}"

pid=$PID_DIR/$APP-${IDENT_STRING}.pid
loggc=$LOG_DIR/$LOG_PREFIX.gc

if [ "$NICENESS" = "" ]; then
 export NICENESS=0
fi
