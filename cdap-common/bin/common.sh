#!/usr/bin/env bash

#
# Copyright © 2014-2015 Cask Data, Inc.
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

# checks if there exists a PID that is already running. return 0 idempotently
check_before_start() {
  if [ -f ${pid} ]; then
    if kill -0 $(<${pid}) > /dev/null 2>&1; then
      echo "${APP} running as process $(<${pid}). Stop it first."
      return 1
    fi
  fi
  return 0
}

create_pid_dir() {
  mkdir -p "${PID_DIR}"
}

die() {
  echo "ERROR: ${*}"
  exit 1
}

# usage: get_conf "explore.enabled" "${CDAP_CONF}"/cdap-site.xml false
get_conf() {
  local __pn=${1} __fn=${2} __default=${3} __result=
  # Check for xmllint
  [[ $(which xmllint 2>/dev/null) ]] || {
    case ${PLATFORM} in
      RHEL) die "Cannot locate xmllint, is libxml2 installed?" ;;
      UBUNTU) die "Cannot locate xmllint, is libxml2-utils installed?" ;;
    esac
    # If we get here, die
    die "Cannot locate xmllint, are XML tools installed?"
  }
  # Get property from file, return last result, if multiple are returned
  __property="cat //configuration/property[name='${__pn}']/value[text()]"
  __sed_fu='/^\//d;s/^.*<value>//;s/<\/value>.*$//'
  __result=$(echo "${__property}" | xmllint --shell "${__fn}" | sed "${__sed_fu}" | tail -n 1)
  # Found result, echo it and return 0
  [[ -n "${__result}" ]] && echo ${__result} && return 0
  # No result, echo default and return 0
  [[ -n "${__default}" ]] && echo ${__default} && return 0
  return 1
}

# Rotates the basic start/stop logs
rotate_log () {
  local log=${1} num=5 prev=0
  [[ -n "${2}" ]] && num=${2}
  if [ -f "${log}" ]; then # rotate logs
    while [ ${num} -gt 1 ]; do
      prev=$((${num} - 1))
      [ -f "${log}.${prev}" ] && mv -f "${log}.${prev}" "${log}.${num}"
      num=${prev}
    done
    mv -f "${log}" "${log}.${num}"
  fi
}

# CDAP kinit using properties from cdap-site.xml
cdap_kinit() {
  local __principal=${CDAP_PRINCIPAL:-$(get_conf "cdap.master.kerberos.principal" "${CDAP_CONF}"/cdap-site.xml)}
  local __keytab=${CDAP_KEYTAB:-$(get_conf "cdap.master.kerberos.keytab" "${CDAP_CONF}"/cdap-site.xml)}
  if [ -z "${__principal}" -o -z "${__keytab}" ]; then
    echo "ERROR: Both cdap.master.kerberos.principal and cdap.master.kerberos.keytab must be configured for Kerberos-enabled clusters!"
    return 1
  fi
  if [ ! -r "${__keytab}" ]; then
    echo "ERROR: Cannot read keytab: ${__keytab}"
    return 1
  fi
  if [[ $(which kinit 2>/dev/null) ]]; then
    # Replace _HOST in principal w/ FQDN, like Hadoop does
    kinit -kt "${__keytab}" "${__principal/_HOST/`hostname -f`}"
    if [[ ! $? ]]; then
      echo "ERROR: Failed executing 'kinit -kt \"${__keytab}\" \"${__principal/_HOST/`hostname -f`}\"'"
      return 1
    fi
  else
    echo "ERROR: Cannot locate kinit! Please, ensure the appropriate Kerberos utilities are installed"
    return 1
  fi
  return 0
}

# Attempts to find JAVA in few ways.
set_java () {
  # Determine the Java command to use to start the JVM.
  if [ -n "${JAVA_HOME}" ] ; then
    if [ -x "${JAVA_HOME}/jre/sh/java" ] ; then
      # IBM's JDK on AIX uses strange locations for the executables
      export JAVA="${JAVA_HOME}/jre/sh/java"
    else
      export JAVA="${JAVA_HOME}/bin/java"
    fi
    if [ ! -x "${JAVA}" ] ; then
      echo "ERROR: JAVA_HOME is set to an invalid directory: ${JAVA_HOME}

Please set the JAVA_HOME variable in your environment to match the
location of your Java installation." >&2
      return 1
    fi
  else
    export JAVA="java"
    which java >/dev/null 2>&1 && return 0
    # If we get here, we've failed this city
    echo "ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH.
Please set the JAVA_HOME variable in your environment to match the
location of your Java installation." >&2
    return 1
  fi
  return 0
}

# Sets the correct HBase support library to use, based on what version exists in the classpath
# NOTE: this function is also sourced and invoked by the CSD control script, found here:
#   https://github.com/caskdata/cm_csd/blob/develop/src/scripts/cdap-control.sh
#   Any changes to this function must be compatible with the CSD's invocation
set_hbase() {
  # Why is this here? Is this not redundant?
  if [ -z "${JAVA}" ]; then
    echo "ERROR: JAVA is not yet set, cannot determine HBase version"
    return 1
  fi

  retvalue=1
  if [ -z "${HBASE_VERSION}" ]; then
    HBASE_VERSION=$(${JAVA} -cp ${CLASSPATH} co.cask.tephra.util.HBaseVersion 2> /dev/null)
    retvalue=$?
  else
    retvalue=0
  fi

  # only set HBase version if previous call succeeded (may fail for components that don't use HBase)
  if [ ${retvalue} == 0 ]; then
    case ${HBASE_VERSION} in
      0.96*)
        hbasecompat="${CDAP_HOME}/hbase-compat-0.96/lib/*"
        ;;
      0.98*)
        hbasecompat="${CDAP_HOME}/hbase-compat-0.98/lib/*"
        ;;
      1.0-cdh*)
        hbasecompat="${CDAP_HOME}/hbase-compat-1.0-cdh/lib/*"
        ;;
      1.0*)
        hbasecompat="${CDAP_HOME}/hbase-compat-1.0/lib/*"
        ;;
      1.1*)
        hbasecompat="$CDAP_HOME/hbase-compat-1.1/lib/*"
        ;;
      *)
        echo "ERROR: Unknown/unsupported version of HBase found: ${HBASE_VERSION}"
        return 1
        ;;
    esac
    if [ -n "${hbasecompat}" ]; then
      CLASSPATH="${hbasecompat}:${CLASSPATH}"
    else
      # When will we ever hit this case?
      echo "ERROR: Failed to find installed hbase-compat jar for version ${HBASE_VERSION}."
      echo "       Is the hbase-compat-* package installed?"
      return 1
    fi
  fi
  export CLASSPATH
  return 0
}

# set the classpath to include hadoop and hbase dependencies
# NOTE: this function is also sourced and invoked by the CSD control script, found here:
#   https://github.com/caskdata/cm_csd/blob/develop/src/scripts/cdap-control.sh
#   Any changes to this function must be compatible with the CSD's invocation
set_classpath() {
  COMP_HOME=${1}
  CCONF=${2}
  if [ -n "${HBASE_HOME}" ]; then
    HBASE_CP=$(${HBASE_HOME}/bin/hbase classpath)
  elif [[ $(which hbase 2>/dev/null) ]]; then
    HBASE_CP=$(hbase classpath)
  fi

  # Where is this used outside this function?
  export HBASE_CP

  if [ -n "${HBASE_CP}" ]; then
    CP="${COMP_HOME}/lib/*:${HBASE_CP}:${CCONF}/:${COMP_HOME}/conf/:${EXTRA_CLASSPATH}"
  else
    # assume Hadoop/HBase libs are included via EXTRA_CLASSPATH
    echo "WARN: could not find Hadoop and HBase libraries"
    CP="${COMP_HOME}/lib/*:${CCONF}/:${COMP_HOME}/conf/:${EXTRA_CLASSPATH}"
  fi

  # Setup classpaths.
  if [ -n "${CLASSPATH}" ]; then
    CLASSPATH="${CLASSPATH}:${CP}"
  else
    CLASSPATH="${CP}"
  fi

  export CLASSPATH
}

# Determine Hive classpath, and set EXPLORE_CLASSPATH.
# Hive classpath is not added as part of system classpath as hive jars bundle unrelated jars like guava,
# and hence need to be isolated.
# NOTE: this function is also sourced and invoked by the CSD control script, found here:
#   https://github.com/caskdata/cm_csd/blob/develop/src/scripts/cdap-control.sh
#   Any changes to this function must be compatible with the CSD's invocation
set_hive_classpath() {
  local __explore=${EXPLORE_ENABLED:-$(get_conf "explore.enabled" "${CDAP_CONF}"/cdap-site.xml false)}
  if [[ "${__explore}" == "true" ]]; then
    if [ -z "${HIVE_HOME}" -o -z "${HIVE_CONF_DIR}" -o -z "${HADOOP_CONF_DIR}" ]; then
      __secure=${KERBEROS_ENABLED:-$(get_conf "kerberos.auth.enabled" "${CDAP_CONF}"/cdap-site.xml false)}
      if [[ "${__secure}" == "true" ]]; then
        cdap_kinit || return 1
      fi

      if [[ $(which hive 2>/dev/null) ]]; then
        ERR_FILE=$(mktemp)
        HIVE_VAR_OUT=$(hive -e 'set -v' 2>${ERR_FILE})
        __ret=$?
        HIVE_ERR_MSG=$(< ${ERR_FILE})
        rm ${ERR_FILE}
        if [ ${__ret} -ne 0 ]; then
          echo "ERROR - Failed getting Hive settings using: hive -e 'set -v':"
          echo "${HIVE_ERR_MSG}"
          return 1
        fi
        HIVE_VARS=$(echo ${HIVE_VAR_OUT} | tr ' ' '\n')
        # Quotes preserve whitespace
        HIVE_HOME=${HIVE_HOME:-$(echo -e "${HIVE_VARS}" | grep '^env:HIVE_HOME=' | cut -d= -f2)}
        HIVE_CONF_DIR=${HIVE_CONF_DIR:-$(echo -e "${HIVE_VARS}" | grep '^env:HIVE_CONF_DIR=' | cut -d= -f2)}
        HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-$(echo -e "${HIVE_VARS}" | grep '^env:HADOOP_CONF_DIR=' | cut -d= -f2)}
      fi
    fi

    # If Hive classpath is successfully determined, derive explore
    # classpath from it and export it to use it in the launch command
    if [ -n "${HIVE_HOME}" -a -n "${HIVE_CONF_DIR}" -a -n "${HADOOP_CONF_DIR}" ]; then
      EXPLORE_CONF_FILES=$(ls -1 ${HIVE_CONF_DIR}/* ${HADOOP_CONF_DIR}/* | tr '\n' ':')
      EXPLORE_CLASSPATH=$(ls -1 ${HIVE_HOME}/lib/hive-exec-* ${HIVE_HOME}/lib/*.jar | tr '\n' ':')
      export EXPLORE_CONF_FILES EXPLORE_CLASSPATH
    fi
  fi
}

# Check that directory /var/tmp/cdap exists in the master node, or create it
check_or_create_master_local_dir() {
  mkdir -p "${LOCAL_DIR}"
}

# check and set classpath if in development enviroment
check_and_set_classpath_for_dev_environment () {
  APP_HOME=${1}

  # Detect if we are in development.
  IN_DEV_ENVIRONMENT=${IN_DEV_ENVIRONMENT:-false}

  # for developers only, add flow and flow related stuff to class path.
  if [[ "${IN_DEV_ENVIRONMENT}" == "true" ]]; then
    echo "Constructing classpath for development environment ..."
    [[ -f "${APP_HOME}"/build/generated-classpath ]] && CLASSPATH+=":$(<${APP_HOME}/build/generated-classpath)"
    [[ -d "${APP_HOME}"/build/classes ]] && CLASSPATH+=":${APP_HOME}/build/classes/main:${APP_HOME}/conf/*"
    [[ -d "${APP_HOME}"/../data-fabric/build/classes ]] && CLASSPATH+=":${APP_HOME}/../data-fabric/build/classes/main"
    [[ -d "${APP_HOME}"/../common/build/classes ]] && CLASSPATH+=":${APP_HOME}/../common/build/classes/main"
    [[ -d "${APP_HOME}"/../gateway/build/classes ]] && CLASSPATH+=":${APP_HOME}/../gateway/build/classes/main"
    export CLASSPATH
  fi
}

HOSTNAME=$(hostname -f)
export LOG_PREFIX=${APP}-${IDENT_STRING}-${HOSTNAME}
export LOGFILE=${LOG_PREFIX}.log
loglog="${LOG_DIR}/${LOGFILE}"

pid=${PID_DIR}/${APP}-${IDENT_STRING}.pid
loggc="${LOG_DIR}/${LOG_PREFIX}.gc"

export NICENESS=${NICENESS:-0}
