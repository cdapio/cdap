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

# Sets the correct HBase support library to use, based on what version exists in the classpath
set_hbase()
{
  if [ -z "$JAVA" ]; then
    echo "ERROR: JAVA is not yet set, cannot determine HBase version"
    exit 1
  fi

  if [ -z "$HBASE_VERSION" ]; then
    HBASE_VERSION=`$JAVA -cp $CLASSPATH com.continuuity.data2.util.hbase.HBaseVersion 2> /dev/null`
    retvalue=$?
  fi

  # only set HBase version if previous call succeeded (may fail for components that don't use HBase)
  if [ $retvalue == 0 ]; then
    case "$HBASE_VERSION" in
      0.94*)
        hbasecompat="$CONTINUUITY_HOME/hbase-compat-0.94/lib/*"
        ;;
      0.96*)
        hbasecompat="$CONTINUUITY_HOME/hbase-compat-0.96/lib/*"
        ;;
      0.98*)
        hbasecompat="$CONTINUUITY_HOME/hbase-compat-0.96/lib/*"
        ;;
      *)
        echo "ERROR: Unknown/unsupported version of HBase found: $HBASE_VERSION"
        exit 1
        ;;
    esac
    if [ -n "$hbasecompat" ]; then
      CLASSPATH=$hbasecompat:$CLASSPATH
    else
      echo "ERROR: Failed to find installed hbase-compat jar for version $HBASE_VERSION."
      echo "       Is the hbase-compat-* package installed?"
    fi
  fi
  export CLASSPATH
}

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

# Determine Hive classpath, and set EXPLORE_CLASSPATH.
# Hive classpath is not added as part of system classpath as hive jars bundle unrelated jars like guava,
# and hence need to be isolated.
set_hive_classpath() {
  if [ "x$HIVE_HOME" = "x" ] || [ "x$HIVE_CONF_DIR" = "x" ]; then
    if [ `which hive 2>/dev/null` ]; then
      HIVE_VAR_OUT=`hive -e 'set -v' 2>/dev/null`

      if [ "x$HIVE_HOME" = "x" ]; then
        HIVE_HOME=`echo $HIVE_VAR_OUT | tr ' ' '\n' | grep 'HIVE_HOME' | cut -f 2 -d '='`
      fi

      if [ "x$HIVE_CONF_DIR" = "x" ]; then
        HIVE_CONF_DIR=`echo $HIVE_VAR_OUT | tr ' ' '\n' | grep 'HIVE_CONF_DIR' | cut -f 2 -d '='`
      fi

      if [ "x$HADOOP_CONF_DIR" = "x" ]; then
        HADOOP_CONF_DIR=`echo $HIVE_VAR_OUT | tr ' ' '\n' | grep 'HADOOP_CONF_DIR=' | cut -f 2 -d '='`
      fi
    fi
  fi

  # If Hive classpath is successfully determined, derive explore
  # classpath from it and export it to use it in the launch command
  if [ "x$HIVE_HOME" != "x" -a "x$HIVE_CONF_DIR" != "x" -a "x$HADOOP_CONF_DIR" != "x" ]; then
    # Reference the conf files needed by Explore
    EXPLORE_CONF_FILES=''
    for a in `ls $HIVE_CONF_DIR`; do
      EXPLORE_CONF_FILES=$EXPLORE_CONF_FILES:$HIVE_CONF_DIR/$a;
    done
    for a in `ls $HADOOP_CONF_DIR`; do
      EXPLORE_CONF_FILES=$EXPLORE_CONF_FILES:$HADOOP_CONF_DIR/$a;
    done
    # Remove leading ':'
    EXPLORE_CONF_FILES=${EXPLORE_CONF_FILES:1:${#EXPLORE_CONF_FILES}-1}
    export EXPLORE_CONF_FILES

    # Hive exec has a HiveConf class that needs to be loaded before the HiveConf class from
    # hive-common for joins operations to work
    HIVE_EXEC=`ls $HIVE_HOME/lib/hive-exec-*`
    OTHER_HIVE_JARS=`ls $HIVE_HOME/lib/*.jar | tr '\n' ':'`

    # We put in the explore classpath all the jars that are not in the regular reactor classpath.
    EXPLORE_CLASSPATH=$HIVE_EXEC:$OTHER_HIVE_JARS

    export EXPLORE_CLASSPATH
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
