#!/usr/bin/env bash
#
# Copyright © 2016-2018 Cask Data, Inc.
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
# This file contains functions used by the cdap script. These functions should
# be usable by other scripts and should be prefixed to prevent namespace issues
# with other scripts. This file will also set CDAP variables in the environment
# when sourced.
#

###
#
# Global functions (not prefixed)
#

#
# die [message] [exit code]
# Outputs error message, then exits with the given exit code, or 1
#
die() { local readonly __code=${2:-1}; echo "[ERROR] ${1}" >&2; exit ${__code}; };

#
# program_is_installed <program>
# Checks for program in PATH and returns true if found
#
program_is_installed() { type ${1} >/dev/null 2>&1; local readonly __ret=${?}; return ${__ret}; };

#
# split_jvm_opts <variable> [variable] [variable]
# Splits multiple variables of JVM options into a JVM_OPTS bash array
#
split_jvm_opts() { JVM_OPTS=(${@}); };

#
# rotate_log <file> [num]
# Rotates a given file through num iterations (default 5)
#
rotate_log() {
  local readonly __log=${1}
  local __num=${2:-5} __prev=0
  if [[ -f ${__log} ]]; then
    while [[ ${__num} -gt 1 ]]; do
      __prev=$((__num - 1))
      test -f "${__log}".${__prev} && mv -f "${__log}".{${__prev},${__num}}
      __num=${__prev}
    done
    mv -f "${__log}"{,.${__num}}
  fi
  return 0
}

#
# compare_versions <version> <version>
# returns: 1 if first is greater, 2 if second is greater, 0 if same
#
compare_versions() {
  [[ ${1} == ${2} ]] && return 0
  local IFS=.
  local i ver1=(${1}) ver2=(${2})
  # fill empty fields in ver1 with zeros
  for ((i=${#ver1[@]}; i<${#ver2[@]}; i++)); do
    ver1[i]=0
  done
  for ((i=0; i<${#ver1[@]}; i++)); do
    if [[ -z ${ver2[i]} ]]; then
      # fill empty fields in ver2 with zeros
      ver2[i]=0
    fi
    if ((10#${ver1[i]} > 10#${ver2[i]})); then
      return 1
    fi
    if ((10#${ver1[i]} < 10#${ver2[i]})); then
      return 2
    fi
  done
  return 0
}

#
# logecho <message>
#
logecho() {
  echo ${@} | tee -a ${__logfile}
}

#
# __readlink <file|directory>
#
__readlink() {
  local __target_file=${1}
  cd $(dirname ${__target_file})
  __target_file=$(basename ${__target_file})
  while test -L ${__target_file}; do
    __target_file=$(readlink ${__target_file})
    cd $(dirname ${__target_file})
    __target_file=$(basename ${__target_file})
  done
  echo "$(pwd -P)/${__target_file}"
}

###
#
# Directory functions
#

# Creates a LOCAL_DIR if it doesn't exist
#
cdap_create_local_dir() { test -d "${LOCAL_DIR}" || mkdir -p "${LOCAL_DIR}"; };

# Creates a LOG_DIR if it doesn't exist
#
cdap_create_log_dir() { test -d "${LOG_DIR}" || mkdir -p "${LOG_DIR}"; };

# Creates a PID_DIR if it doesn't exist
#
cdap_create_pid_dir() { test -d "${PID_DIR}" || mkdir -p "${PID_DIR}"; };

cdap_create_dir() { test -d "${1}" || mkdir -p "${1}"; }

# Locates CDAP_HOME and returns its location
#
cdap_home() {
  if [[ -n ${CDAP_HOME} ]] && [[ -d ${CDAP_HOME} ]]; then
    echo ${CDAP_HOME}
    return 0
  fi
  local readonly __script=${BASH_SOURCE[0]}
  local readonly __dirname=$(dirname "${__script}")
  local readonly __script_bin=$(cd "${__dirname}"; pwd -P)
  local readonly __comp_home=$(cd "${__script%/*/*}" >&-; pwd -P)
  if [[ ${__comp_home%/*} == /opt/cdap ]] && [[ ${__comp_home} != /opt/cdap/sdk* ]] && [[ ${__comp_home} != /opt/cdap/sandbox* ]]; then
    __app_home=${__comp_home}
    __cdap_home=/opt/cdap
  elif [[ ${__comp_home##*/} == cli ]]; then
    __app_home=${__comp_home}
    __cdap_home=${__comp_home%/*}
  else
    __app_home=$(dirname "${__script_bin}")
    __cdap_home=${__app_home}
  fi
  echo ${__cdap_home}
}

###
#
# Service/Process manipulation via PID file
#

#
# cdap_status_pidfile <pidfile>
# returns: 3 if file exists but process not running, 2 if no process, 0 if running, 1 otherwise
#
cdap_status_pidfile() {
  local readonly __pidfile=${1} __label=${2:-Process}
  if [[ -f ${__pidfile} ]]; then
    local readonly __pid=$(<${__pidfile})
    if kill -0 ${__pid} >/dev/null 2>&1; then
      echo "${__label} running as PID ${__pid}"
      return 0
    else
      echo "PID file ${__pidfile} exists, but process ${__pid} does not appear to be running"
      return 3
    fi
  else
    echo "${__label} is not running"
    return 2
  fi
  return 1
}

#
# cdap_stop_pidfile <pidfile>
# returns: exit code
#
cdap_stop_pidfile() {
  local readonly __ret __pidfile=${1} __label=${2:-Process}
  if [[ -f ${__pidfile} ]]; then
    local readonly __pid=$(<${__pidfile})
    echo -n "$(date) Stopping ${__label} ..."
    if kill -0 ${__pid} >/dev/null 2>&1; then
      kill ${__pid} >/dev/null 2>&1
      while kill -0 ${__pid} >/dev/null 2>&1; do
        echo -n .
        sleep 1
      done
      rm -f ${__pidfile}
      echo
      __ret=0
    else
      __ret=${?}
    fi
    echo
  fi
  return ${__ret}
}

cdap_kill_pidfile() {
  local readonly __ret __pidfile=${1} __label=${2:-Process}
  if [[ -f ${__pidfile} ]]; then
    local readonly __pid=$(<${__pidfile})
    echo -n "$(date) Killing ${__label} ..."
    if kill -0 ${__pid} >/dev/null 2>&1; then
      kill -9 ${__pid} >/dev/null 2>&1
      while kill -0 ${__pid} >/dev/null 2>&1; do
        echo -n .
        sleep 1
      done
      rm -f ${__pidfile}
      echo
      __ret=0
    else
      __ret=${?}
    fi
    echo
  fi
  return ${__ret}
}

#
# cdap_check_pidfile <pidfile> [label]
# returns: 1 on error, 0 otherwise
#
cdap_check_pidfile() {
  local readonly __pidfile=${1} __label=${2:-Process}
  local readonly __ret
  cdap_status_pidfile ${__pidfile} ${__label} > /dev/null
  __ret=$?
  case ${__ret} in
    0) echo "$(date) Please stop CDAP ${__label} running as process $(<${__pidfile}) first, or use the restart function" ;;
    *) return 0 ;;
  esac
  return 1
}

###
#
# CDAP helper functions
#

#
# cdap_check_node_version <version>
# returns: 1 if not found or not high enough version, 0 otherwise
#
cdap_check_node_version() {
  local readonly __ver=${1/v/} __ret
  program_is_installed node || die "Cannot locate node, is Node.js installed?"
  local readonly __node=$(node -v 2>/dev/null | sed -e 's/v//g')
  compare_versions ${__node} ${__ver}
  __ret=$?
  case ${__ret} in
    0|1) return 0 ;;
    *) echo "Node.js ${__node} is not supported. The minimum version supported is ${__ver}" ; return 1 ;;
  esac
  return 0
}

#
# cdap_check_mapr
# returns: 0 if MapR is detected, 1 otherwise
#
cdap_check_mapr() {
  if [[ -f /opt/mapr/MapRBuildVersion ]]; then
    return 0
  fi
  return 1
}

#
# cdap_get_conf <property> <conf-file> [default]
# returns: property value if found, default if not found and default set, otherwise returns 1
#
cdap_get_conf() {
  local readonly __pn=${1} __fn=${2} __default=${3} __result __property __sed_fu
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
  [[ -n ${__result} ]] && echo ${__result} && return 0
  # No result, echo default and return 0
  [[ -n ${__default} ]] && echo ${__default} && return 0
  return 1
}

#
# cdap_kinit
# Initializes Kerberos ticket using principal/keytab
#
cdap_kinit() {
  local readonly __principal=${CDAP_PRINCIPAL:-$(cdap_get_conf "cdap.master.kerberos.principal" "${CDAP_CONF}"/cdap-site.xml)}
  local readonly __keytab=${CDAP_KEYTAB:-$(cdap_get_conf "cdap.master.kerberos.keytab" "${CDAP_CONF}"/cdap-site.xml)}
  if [[ -z ${__principal} ]] || [[ -z ${__keytab} ]]; then
    die "Both cdap.master.kerberos.principal and cdap.master.kerberos.keytab must be configured for Kerberos"
  fi
  if [[ ! -r ${__keytab} ]]; then
    die "Cannot read keytab: ${__keytab}"
  fi
  if [[ $(which kinit 2>/dev/null) ]]; then
    # Replace _HOST in principal w/ FQDN, like Hadoop does
    kinit -kt "${__keytab}" "${__principal/_HOST/${HOSTNAME}}"
    if [[ ! ${?} ]]; then
      die "Failed executing 'kinit -kt \"${__keytab}\" \"${__principal/_HOST/${HOSTNAME}}\"'"
    fi
  else
    die "Cannot locate kinit! Please, ensure the appropriate Kerberos utilities are installed"
  fi
  return 0
}

#
# cdap_set_java
# Attempts to find JAVA in few ways and sets JAVA variable
#
cdap_set_java () {
  local readonly __java __java_version
  # Check JAVA_HOME, first
  if [[ -n ${JAVA_HOME} ]] && [[ -d ${JAVA_HOME} ]]; then
    __java="${JAVA_HOME}"/bin/java
    [[ -x ${__java} ]] || die "JAVA_HOME is set to an invalid location: ${JAVA_HOME}"
  else
    __java=${JAVA:-java}
    if [[ ! $(which java 2>/dev/null) ]]; then
      die "JAVA_HOME is not set and 'java' was not found in your PATH. Please set JAVA_HOME to the location of your Java install"
    fi
  fi
  __java_version=$("${__java}" -version 2>&1 | grep version | awk '{print $3}' | awk -F '.' '{print $2}')
  if [[ -z ${__java_version} ]]; then
    die "Could not detect Java version. Aborting..."
  elif [[ ${__java_version} -lt 8 ]]; then
    die "Java version not supported. Please install Java 8 - other versions of Java are not supported."
  fi
  export JAVA=${__java}
  return 0
}

#
# cdap_set_classpath <home-dir> <conf-dir> [verbose: true/false]
# Assembles CLASSPATH from home-dir, hbase classpath, and conf-dir and optionally echoes if verbose is set true
# NOTE: this function is also sourced and invoked by the CSD control script, found here:
#   https://github.com/caskdata/cm_csd/blob/develop/src/scripts/cdap-control.sh
#   Any changes to this function must be compatible with the CSD's invocation
#
cdap_set_classpath() {
  local readonly __home=${1} __conf=${2} __verbose=${3:-false}
  local readonly __homelib=$(find -L "${__home}"/lib -type f 2>/dev/null | sort | tr '\n' ':')
  local __cp __hbase_cp

  # Get HBase's CLASSPATH
  if [[ -n ${HBASE_CLASSPATH} ]] && [[ ${HBASE_CLASSPATH} != '' ]]; then
    __cp=${__homelib}:${HBASE_CLASSPATH}:${__conf}/:${__home}/conf/:${EXTRA_CLASSPATH}
  elif [[ -n ${HBASE_HOME} ]] && [[ -d ${HBASE_HOME} ]]; then
    __hbase_cp=$("${HBASE_HOME}"/bin/hbase classpath)
  elif [[ $(which hbase 2>/dev/null) ]]; then
    __hbase_cp=$(hbase classpath)
  elif [[ -n ${HADOOP_HOME} ]] && [[ -d ${HADOOP_HOME} ]]; then
    # For the no hbase case, we still want to setup the Hadoop classpath
    __hbase_cp=$("${HADOOP_HOME}"/bin/hadoop classpath)
  else
    # assume Hadoop/HBase libs are included via EXTRA_CLASSPATH
    logecho "[WARN] Could not find Hadoop and HBase libraries, using EXTRA_CLASSPATH"
    __cp=${__homelib}:${__conf}/:${__home}/conf/:${EXTRA_CLASSPATH}
  fi
  # Add HBase's CLASSPATH, if found and not provided
  if [[ -n ${__hbase_cp} ]] && [[ -z ${__cp} ]]; then
    __cp=${__homelib}:${__hbase_cp}:${__conf}/:${__home}/conf/:${EXTRA_CLASSPATH}
  fi
  if [[ -n ${CLASSPATH} ]]; then
    CLASSPATH+=:${__cp}
  else
    CLASSPATH=${__cp}
  fi
  export CLASSPATH
  if [[ ${__verbose} == 'true' ]]; then
    echo ${CLASSPATH}
  fi
  return 0
}

#
# cdap_set_hbase
# Sets the correct HBase support library to use, based on what version exists in the CLASSPATH
# NOTE: this function is also sourced and invoked by the CSD control script, found here:
#   https://github.com/caskdata/cm_csd/blob/develop/src/scripts/cdap-control.sh
#   Any changes to this function must be compatible with the CSD's invocation
#
cdap_set_hbase() {
  local readonly __compat __compatlib __class=io.cdap.cdap.data2.util.hbase.HBaseVersion
  HBASE_VERSION=${HBASE_VERSION:-$("${JAVA}" -cp ${CLASSPATH} ${__class} 2>/dev/null)}
  case ${HBASE_VERSION} in
    1.0-cdh5.5*|1.0-cdh5.6*) __compat=hbase-compat-1.0-cdh5.5.0 ;; # 5.5 and 5.6 are compatible
    1.0-cdh*) __compat=hbase-compat-1.0-cdh ;;
    1.0*) __compat=hbase-compat-1.0 ;;
    1.1*) __compat=hbase-compat-1.1 ;;
    1.2-cdh*) __compat=hbase-compat-1.2-cdh5.7.0 ;; # 5.7 and 5.8 are compatible
    1.2*) __compat=hbase-compat-1.1 ;; # 1.1 and 1.2 are compatible
    "") die "Unable to determine HBase version! Aborting." ;;
    *)
      if [[ $(cdap_get_conf "hbase.version.resolution.strategy" "${CDAP_CONF}"/cdap-site.xml auto.strict) == 'auto.latest' ]]; then
        local readonly __latest_hbase_compat
        if [[ ${HBASE_VERSION} =~ -cdh ]]; then
          __compat=hbase-compat-1.2-cdh5.7.0 # must be updated if a new CDH HBase version is added
        else
          __compat=hbase-compat-1.1 # must be updated if a new HBase version is added
        fi
        echo "Using ${__compat} for HBase version ${HBASE_VERSION} due to 'auto.latest' resolution strategy."
      else
        die "Unknown or unsupported HBase version found: ${HBASE_VERSION}"
      fi
      ;;
  esac
  __compatlib=$(find -L "${CDAP_HOME}"/${__compat}/lib -type f 2>/dev/null | sort | tr '\n' ':')
  export CLASSPATH="${__compatlib}"${CLASSPATH}
  return 0
}


#
# cdap_set_spark
# Attempts to find SPARK_HOME and setup Spark specifics env by running $SPARK_HOME/conf/spark-env.sh
#
cdap_set_spark() {
  local readonly __saved_stty=$(stty -g 2>/dev/null)
  # If SPARK_HOME is either not set or is not directory, tries to auto-detect it
  if [[ -z ${SPARK_HOME} ]] || [[ ! -d ${SPARK_HOME} ]]; then
    if cdap_check_mapr; then
      # MapR installs spark to a known location
      SPARK_HOME=$(ls -d /opt/mapr/spark/spark-* 2>/dev/null)
      if [[ -z ${SPARK_HOME} ]] || [[ ! -d ${SPARK_HOME} ]]; then
        return 1
      fi
    elif [[ $(which spark-shell 2>/dev/null) ]]; then
      # If there is no valid SPARK_HOME, we should unset the existing one if it is set
      # Otherwise the spark-shell won't run correctly
      unset SPARK_HOME
      local __spark_shell=$(which spark-shell 2>/dev/null)
      local __spark_client_version=None
      for __dist in hdp iop; do
        if [[ ! -d /usr/${__dist} ]]; then
          continue
        fi
        if [[ $(which ${__dist}-select 2>/dev/null) ]]; then
          __spark_name="spark"
          if [[ ${SPARK_MAJOR_VERSION} -ne 1 ]]; then
            __spark_name="spark${SPARK_MAJOR_VERSION}"
          fi
          __spark_client_version=$(${__dist}-select status ${__spark_name}-client | awk '{print $3}')
          if [[ ${__spark_client_version} == 'None' ]]; then # defaults None, we're hoping for a version
            logecho "$(date) Spark client not installed via ${__dist}-select detection"
            return 1
          elif [[ -x /usr/${__dist}/${__spark_client_version}/${__spark_name}/bin/spark-shell ]]; then
            __spark_shell=/usr/${__dist}/${__spark_client_version}/${__spark_name}/bin/spark-shell
          else
            logecho "$(date) Spark client not installed via on-disk detection"
            return 1
          fi
        fi
      done
      ERR_FILE=$(mktemp)
      SPARK_VAR_OUT=$(echo '(sys.env ++ Map(("sparkVersion", org.apache.spark.SPARK_VERSION),("scalaVersion", scala.util.Properties.releaseVersion.get))).foreach { case (k, v) => println(s"$k=$v") }; sys.exit' | ${__spark_shell} --master local 2>${ERR_FILE})
      __ret=$?
      # spark-shell invocation above does not properly restore the stty.
      stty ${__saved_stty} 2>/dev/null
      SPARK_ERR_MSG=$(< ${ERR_FILE})
      rm ${ERR_FILE}
      if [[ ${__ret} -ne 0 ]]; then
        echo "[ERROR] While determining Spark home, failed to get Spark settings using: ${__spark_shell} --master local"
        echo "  stderr:"
        echo "${SPARK_ERR_MSG}"
        return 1
      fi

      SPARK_HOME=$(echo -e "${SPARK_VAR_OUT}" | grep ^SPARK_HOME= | cut -d= -f2)
      cdap_set_spark_compat "${SPARK_VAR_OUT}"
    fi
  fi

  if [[ -z ${SPARK_COMPAT} ]]; then
    cdap_set_spark_compat_with_spark_shell "${SPARK_HOME}"
  fi

  export SPARK_HOME

  # Find environment variables setup via spark-env.sh
  cdap_load_spark_env || logecho "[WARN] Fail to source spark-env.sh to setup environment variables for Spark"
  return 0
}

cdap_load_spark_env() {
  # The same logic as used by Spark to find spark-env.sh
  # When this method is called, SPARK_HOME should already been set,
  # as this method should only be called from cdap_set_spark()
  if [[ -z ${SPARK_HOME} ]]; then
    return 1
  fi

  CONF_DIR="${SPARK_CONF_DIR:-"$SPARK_HOME"/conf}"
  if [[ -f "${CONF_DIR}/spark-env.sh" ]]; then
    if [[ -z ${SPARK_ENV_PATTERN} ]]; then
      # By default, only get env from spark-env.sh that starts with PY or SPARK
      SPARK_ENV_PATTERN="^(PY|SPARK)"
    fi
    SPARK_ENV=$(source ${CONF_DIR}/spark-env.sh; env | grep -E "${SPARK_ENV_PATTERN}")
    __ret=$?
    if [[ ${__ret} -ne 0 ]]; then
      return 1
    fi

    while read -r line; do
      # Prefix the env variable with _SPARK_ to avoid conflicts
      export "_SPARK_${line%%=*}"="${line#*=}"
    done <<< "${SPARK_ENV}"
  fi

  return 0
}

#
# Attempts to determine the spark version and set the SPARK_COMPAT env variable for the CDAP master to use
# by running spark-shell
#
cdap_set_spark_compat_with_spark_shell() {
  local readonly __spark_home=${1}
  local readonly __saved_stty=$(stty -g 2>/dev/null)
  # If SPARK_COMPAT is not already set, try to determine it
  if [[ -z ${SPARK_COMPAT} ]]; then
    ERR_FILE=$(mktemp)
    SPARK_VAR_OUT=$(echo 'Map(("sparkVersion", org.apache.spark.SPARK_VERSION),("scalaVersion", scala.util.Properties.releaseVersion.get)).foreach { case (k, v) => println(s"$k=$v") }; sys.exit' | ${__spark_home}/bin/spark-shell --master local 2>${ERR_FILE})

    __ret=$?
    # spark-shell invocation above does not properly restore the stty.
    stty ${__saved_stty}
    SPARK_ERR_MSG=$(< ${ERR_FILE})
    rm ${ERR_FILE}
    if [[ ${__ret} -ne 0 ]]; then
      echo "[ERROR] Failed to get Spark and Scala versions using spark-shell"
      echo "  stderr:"
      echo "${SPARK_ERR_MSG}"
      return 1
    fi

    cdap_set_spark_compat "${SPARK_VAR_OUT}"
  fi

  return 0
}

#
# Set the SPARK_VERSION and SPARK_COMPAT based on output from spark-shell
#
cdap_set_spark_compat() {
  local __output=${1}
  SPARK_VERSION=$(echo -e "${__output}" | grep "^sparkVersion=" | cut -d= -f2)
  SPARK_MAJOR_VERSION=${SPARK_MAJOR_VERSION:-$(echo ${SPARK_VERSION} | cut -d. -f1)}
  SCALA_VERSION=$(echo -e "${__output}" | grep "^scalaVersion=" | cut -d= -f2)
  SCALA_MAJOR_VERSION=$(echo ${SCALA_VERSION} | cut -d. -f1)
  SCALA_MINOR_VERSION=$(echo ${SCALA_VERSION} | cut -d. -f2)
  SPARK_COMPAT="spark${SPARK_MAJOR_VERSION}_${SCALA_MAJOR_VERSION}.${SCALA_MINOR_VERSION}"

  export SPARK_VERSION
  export SPARK_COMPAT
  return 0
}

#
# cdap_service <service> <action> [arguments]
# Used for interacting with CDAP services where action is one of start/stop/status/restart/condrestart/classpath
#
cdap_service() {
  local readonly __service=${1} __action=${2}
  shift; shift
  local readonly __args=${@}
  local readonly __pidfile=${PID_DIR}/${__service}-${IDENT_STRING}.pid
  local readonly __gc_log_and_heapdump_dir=${LOG_DIR}/${__service}-${IDENT_STRING}
  local readonly __log_prefix=${LOG_DIR}/${__service}-${IDENT_STRING}-${HOSTNAME}
  local readonly __logfile=${__log_prefix}.log
  local readonly __svc=${__service/-server/}
  local readonly __ret

  # awk taken from http://stackoverflow.com/a/1541178
  local __name=$(echo ${__service/-/ } | awk '{for(i=1;i<=NF;i++){ $i=toupper(substr($i,1,1)) substr($i,2) }}1')

  case ${__service} in
    auth-server) local readonly __comp_home="security" ;;
    kafka-server) local readonly __comp_home=${__svc} ;;
    router) local readonly __comp_home="gateway" ;;
    *) local readonly __comp_home=${__service} ;;
  esac
  [[ ${__service} == ui ]] && __name="UI"

  cdap_create_log_dir

  case ${__action} in
    status|stop|kill) cdap_${__action}_pidfile ${__pidfile} "CDAP ${__name}"; __ret=${?} ;;
    start|restart|condrestart)
      if [[ ${__action} == condrestart ]]; then
        cdap_status_pidfile ${__pidfile} "CDAP ${__name}" >/dev/null && \
          cdap_stop_pidfile ${__pidfile} "CDAP ${__name}" && \
          cdap_${__svc} ${__action} ${__args}
      elif [[ ${__action} == restart ]]; then
          cdap_stop_pidfile ${__pidfile} "CDAP ${__name}" ; \
          cdap_${__svc} ${__action} ${__args}
      else
          cdap_${__svc} ${__action} ${__args}
      fi
      __ret=${?}
      ;;
    classpath)
      cdap_set_classpath "${CDAP_HOME}"/${__comp_home} "${CDAP_CONF}"
      [[ ${__service} == master ]] && cdap_set_java && cdap_set_hbase
      echo ${CLASSPATH}
      __ret=0
      ;;
    run) cdap_run_class ${__args} ; __ret=${?} ;;
    exec) cdap_exec_class ${__args} ; __ret=${?} ;;
    usage|-h|--help) echo "Usage: $0 ${__service} {start|stop|restart|status|condrestart|classpath|run|exec}"; __ret=0 ;;
    *) die "Usage: $0 ${__service} {start|stop|restart|status|condrestart|classpath|run|exec}" ;;
  esac
  return ${__ret}
}

#
# cdap_start_bin [args]
# Start a non-Java application with arguments in the background
#
cdap_start_bin() {
  local readonly __args=${@}
  local readonly __svc=${CDAP_SERVICE/-server/}
  local readonly __ret __pid
  local readonly __name=$(if [[ ${__svc} == ui ]]; then echo UI ; else echo ${__svc/-/ } | awk '{for(i=1;i<=NF;i++){ $i=toupper(substr($i,1,1)) substr($i,2) }}1' ; fi)
  cdap_check_pidfile ${__pidfile} ${__name} || exit 0 # Error output is done in function
  cdap_create_pid_dir || die "Could not create PID dir: ${PID_DIR}"
  logecho "$(date) Starting CDAP ${__name} service on ${HOSTNAME}"
  ulimit -a >>${__logfile} 2>&1
  nohup nice -n ${NICENESS} ${MAIN_CMD} ${MAIN_CMD_ARGS} ${__args} </dev/null >>${__logfile} 2>&1 &
  __pid=${!}
  __ret=${?}
  echo ${__pid} >${__pidfile}
  if ! kill -0 ${__pid} >/dev/null 2>&1; then
    die "${MAIN_CMD} failed to start, please check logs at ${LOG_DIR} for more information"
  fi
  return ${__ret}
}

#
# cdap_run_bin [args]
# Runs a non-Java application with arguments in the foreground
#
cdap_run_bin() {
  local readonly __bin=${1}
  shift
  local readonly __args=${@}
  local readonly __ret
  ${__bin} ${__args}
  __ret=${?}
  return ${__ret}
}

#
# cdap_start_java [args]
# Start a Java application from class name with arguments in the background
#
cdap_start_java() {
  local readonly __name=$(echo ${CDAP_SERVICE/-/ } | awk '{for(i=1;i<=NF;i++){ $i=toupper(substr($i,1,1)) substr($i,2) }}1')
  cdap_check_pidfile ${__pidfile} ${__name} || exit 0 # Error output is done in function
  cdap_create_pid_dir || die "Could not create PID dir: ${PID_DIR}"
  # Check and set classpath if in development environment.
  cdap_check_and_set_classpath_for_dev_environment "${CDAP_HOME}"
  # Setup classpaths.
  cdap_set_classpath "${CDAP_HOME}"/${__comp_home} "${CDAP_CONF}"
  # Setup Java
  cdap_set_java || return 1
  # Set JAVA_HEAPMAX from variable defined in JAVA_HEAP_VAR, unless defined already
  JAVA_HEAPMAX=${JAVA_HEAPMAX:-${!JAVA_HEAP_VAR}}
  export JAVA_HEAPMAX
  # Split JVM_OPTS array
  eval split_jvm_opts ${!JAVA_OPTS_VAR} ${OPTS} ${JAVA_OPTS}
  local __defines="-Dcdap.service=${CDAP_SERVICE} ${JAVA_HEAPMAX} -Duser.dir=${LOCAL_DIR} -Djava.io.tmpdir=${TEMP_DIR}"
  # Enable GC logging
  cdap_create_dir ${__gc_log_and_heapdump_dir}
  if [[ ${HEAPDUMP_ON_OOM} == true ]]; then
    __defines+=" -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${__gc_log_and_heapdump_dir}"
  fi
  __defines+=" -verbose:gc -Xloggc:${__gc_log_and_heapdump_dir}/gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=1M"
  logecho "$(date) Starting CDAP ${__name} service on ${HOSTNAME}"
  echo
  if [[ ${CDAP_SERVICE} == master ]]; then
    # Determine SPARK_HOME
    cdap_set_spark || logecho "$(date) Could not determine SPARK_HOME! Spark support unavailable!"
    if [[ -n ${SPARK_COMPAT} ]]; then
      __defines+=" -Dapp.program.spark.compat=${SPARK_COMPAT}"
    fi
    # Master requires setting hive classpath
    cdap_set_hive_classpath || return 1
    # Add proper HBase compatibility to CLASSPATH
    cdap_set_hbase || return 1
    # Master requires this local directory
    cdap_create_local_dir || die "Could not create Master local directory"
    # Check for JAVA_LIBRARY_PATH
    if [[ -n ${JAVA_LIBRARY_PATH} ]]; then
      __defines+=" -Djava.library.path=${JAVA_LIBRARY_PATH}"
    fi
    # Check for HDP 2.2+ or IOP, otherwise do nothing and leave up to the user to configure
    for __dist in hdp iop; do
      if [[ $(which ${__dist}-select 2>/dev/null) ]]; then
        local __auto_version=$(${__dist}-select status hadoop-client | awk '{print $3}')
        # Check for version configured in OPTS
        if [[ ${OPTS} =~ -D${__dist}.version ]]; then
          local __conf_version=$(echo ${OPTS} | grep -oP "\-D${__dist}.version=\d+\.\d+\.\d+\.\d+-\d+" | cut -d= -f2)
          if [[ ${__conf_version} != ${__auto_version} ]]; then
            local __caps=$(echo ${__dist} | awk 'BEGIN { getline; print toupper($0) }')
            logecho "$(date) [WARN] ${__caps} version mismatch! Detected: ${__auto_version}, Configured: ${__conf_version}"
            logecho "$(date) [WARN] Using configured ${__caps} version: ${__conf_version}"
          fi
        else
          # No version specified in OPTS or incorrect format, appending ours
          __defines+=" -D${__dist}.version=${__auto_version}"
          logecho "$(date) Detected ${__dist} version ${__auto_version} and adding to CDAP Master command line"
        fi
      fi
    done

    # Build and upload coprocessor jars
    logecho "$(date) Ensuring required HBase coprocessors are on HDFS"
    cdap_setup_coprocessors </dev/null >>${__logfile} 2>&1 || die "Could not setup coprocessors. Please check ${__logfile} for more information."

    __startup_checks=${CDAP_STARTUP_CHECKS:-$(cdap_get_conf "master.startup.checks.enabled" "${CDAP_CONF}"/cdap-site.xml true)}
    if [[ ${__startup_checks} == true ]]; then
      logecho "$(date) Running CDAP Master startup checks -- this may take a few minutes"
      "${JAVA}" ${JAVA_HEAPMAX} ${JVM_OPTS[@]} -cp ${CLASSPATH} io.cdap.cdap.master.startup.MasterStartupTool </dev/null >>${__logfile} 2>&1
      if [ $? -ne 0 ]; then
        die "Master startup checks failed. Please check ${__logfile} to address issues."
      fi
    fi
  fi
  "${JAVA}" -version 2>>${__logfile}
  ulimit -a >>${__logfile}
  __defines+=" ${JVM_OPTS[@]}"
  echo "$(date) Running: ${JAVA} ${__defines} -cp ${CLASSPATH} ${MAIN_CLASS} ${MAIN_CLASS_ARGS} ${@}" >>${__logfile}
  # Start our JVM
  nohup nice -n ${NICENESS} "${JAVA}" ${__defines} -cp ${CLASSPATH} ${MAIN_CLASS} ${MAIN_CLASS_ARGS} ${@} </dev/null >>${__logfile} 2>&1 &
  echo $! >${__pidfile}
  sleep 2 # Now, wait for JVM spinup
  kill -0 $(<${__pidfile}) >/dev/null 2>&1
  return $?
}

#
# cdap_run_class <class> [arguments]
# Executes a given class' main method with the CLASSPATH and environment setup
#
cdap_run_class() {
  local readonly __class=${1}
  shift
  local readonly __args=${@}
  local readonly __ret
  local JAVA_HEAPMAX=${JAVA_HEAPMAX:--Xmx1024m}
  [[ -z ${__class} ]] && echo "[ERROR] No class name given!" && die "Usage: ${0} run <fully-qualified-class> [arguments]"
  # Check and set classpath if in development environment.
  cdap_check_and_set_classpath_for_dev_environment "${CDAP_HOME}"
  # Setup classpaths.
  cdap_set_classpath "${CDAP_HOME}"/master "${CDAP_CONF}"
  # Setup Java
  cdap_set_java || return 1
  cdap_set_spark || logecho "$(date) [WARN] Could not determine SPARK_HOME! Spark support unavailable!"
  cdap_set_hive_classpath || return 1
  # Add proper HBase compatibility to CLASSPATH
  cdap_set_hbase || exit 1
  cdap_create_local_dir || die "Could not create local directory"
  if [[ -n ${__args} ]] && [[ ${__args} != '' ]]; then
    echo "$(date) Running class ${__class} with arguments: ${__args}"
  else
    echo "$(date) Running class ${__class}"
  fi
  "${JAVA}" ${JAVA_HEAPMAX} -Dhive.classpath=${HIVE_CLASSPATH} -Duser.dir=${LOCAL_DIR} -Djava.io.tmpdir=${TEMP_DIR} ${OPTS} -cp ${CLASSPATH} ${__class} ${__args}
  __ret=${?}
  return ${__ret}
}

#
# cdap_exec_class <class> [arguments]
# Executes a given class' main method with the CLASSPATH and environment setup. It replaces the current process
# with the new Java process
#
cdap_exec_class() {
  local readonly __class=${1}
  shift
  local readonly __args=${@}
  local JAVA_HEAPMAX=${JAVA_HEAPMAX:--Xmx1024m}
  [[ -z ${__class} ]] && echo "[ERROR] No class name given!" && die "Usage: ${0} run <fully-qualified-class> [arguments]"
  # Check and set classpath if in development environment.
  cdap_check_and_set_classpath_for_dev_environment "${CDAP_HOME}"
  # Setup classpaths.
  cdap_set_classpath "${CDAP_HOME}"/master "${CDAP_CONF}"
  # Setup Java
  cdap_set_java || return 1
  cdap_set_spark || logecho "$(date) [WARN] Could not determine SPARK_HOME! Spark support unavailable!"
  cdap_set_hive_classpath || return 1
  # Add proper HBase compatibility to CLASSPATH
  cdap_set_hbase || exit 1
  cdap_create_local_dir || die "Could not create local directory"
  if [[ -n ${__args} ]] && [[ ${__args} != '' ]]; then
    echo "$(date) Running class ${__class} with arguments: ${__args}"
  else
    echo "$(date) Running class ${__class}"
  fi
  exec "${JAVA}" ${JAVA_HEAPMAX} -Dhive.classpath=${HIVE_CLASSPATH} -Duser.dir=${LOCAL_DIR} -Djava.io.tmpdir=${TEMP_DIR} ${OPTS} -cp ${CLASSPATH} ${__class} ${__args}
}

#
# cdap_check_and_set_classpath_for_dev_environment <home-dir>
# check and set classpath if in development enviroment
#
cdap_check_and_set_classpath_for_dev_environment() {
  local readonly __home=${1}

  # Detect if we are in development.
  IN_DEV_ENVIRONMENT=${IN_DEV_ENVIRONMENT:-false}

  # for developers only, add flow and flow related stuff to class path.
  if [[ ${IN_DEV_ENVIRONMENT} == true ]]; then
    logecho "Constructing classpath for development environment ..."
    [[ -f "${__home}"/build/generated-classpath ]] && CLASSPATH+=":$(<${__home}/build/generated-classpath)"
    [[ -d "${__home}"/build/classes ]] && CLASSPATH+=":${__home}/build/classes/main:${__home}/conf/*"
    [[ -d "${__home}"/../data-fabric/build/classes ]] && CLASSPATH+=":${__home}/../data-fabric/build/classes/main"
    [[ -d "${__home}"/../common/build/classes ]] && CLASSPATH+=":${__home}/../common/build/classes/main"
    [[ -d "${__home}"/../gateway/build/classes ]] && CLASSPATH+=":${__home}/../gateway/build/classes/main"
    export CLASSPATH
  fi
  return 0
}

#
# cdap_context
# returns "distributed" or "sdk" based on current CDAP_HOME
#
cdap_context() {
  local readonly __context __version=$(cdap_version)
  if [[ -e ${CDAP_HOME}/lib/io.cdap.cdap.cdap-standalone-${__version}.jar ]]; then
    __context=sdk
  else
    __context=distributed
  fi
  echo ${__context}
}

#
# cdap_version [component]
# returns the version of CDAP or <component> in CDAP_HOME, replacing snapshot timestamps with -SNAPSHOT
#
cdap_version() {
  local readonly __component=${1}
  local readonly __cdap_major __cdap_minor __cdap_patch __cdap_snapshot
  local __version
  if [[ -z ${__component} ]]; then
    __version=$(<"${CDAP_HOME}"/VERSION)
  else
    __version=$(<"${CDAP_HOME}"/${__component}/VERSION)
  fi
  __cdap_major=$(echo ${__version} | cut -d. -f1)
  __cdap_minor=$(echo ${__version} | cut -d. -f2)
  __cdap_patch=$(echo ${__version} | cut -d. -f3)
  __cdap_snapshot=$(echo ${__version} | cut -d. -f4)
  if [[ -z ${__cdap_snapshot} ]]; then
    __version=${__cdap_major}.${__cdap_minor}.${__cdap_patch}
  else
    __version=${__cdap_major}.${__cdap_minor}.${__cdap_patch}-SNAPSHOT
  fi
  echo ${__version}
}

#
# cdap_version_command
# returns the version of CDAP SDK or all locally installed CDAP Distributed components
#
cdap_version_command() {
  local readonly __context=$(cdap_context)
  local __component __name
  if [[ ${__context} == sdk ]]; then
    echo "CDAP Sandbox version $(cdap_version)"
    echo
  else # Distributed, possibly CLI-only
    if [[ -r ${CDAP_HOME}/VERSION ]]; then
      echo "CDAP configuration version $(cdap_version)"
    fi
    for __component in cli gateway kafka master security ui; do
      if [[ -r ${CDAP_HOME}/${__component}/VERSION ]]; then
        echo "CDAP ${__component} version $(cdap_version ${__component})"
      fi
    done
    echo
  fi
  return 0
}

###
#
# CDAP SDK functions
#

#
# cdap_sdk_usage
# Outputs usage for the CDAP SDK
# returns: true
#
cdap_sdk_usage() {
  echo
  echo "Usage: ${0} sandbox {start|stop|restart|status|usage}"
  echo
  echo "Additional options with start, restart:"
  echo "--enable-debug [ <port> ] to connect to a debug port for CDAP Sandbox (default port is 5005)"
  echo "--foreground to run the Sandbox in the foreground, showing logs on STDOUT"
  echo
  return 0
}

#
# cdap_sdk_cleanup
# Deletes logs and data from CDAP_HOME
#
cdap_sdk_cleanup() { echo "Removing ${LOCAL_DIR} and ${LOG_DIR}"; rm -rf "${LOCAL_DIR}" "${LOG_DIR}"; };

#
# cdap_sdk_restart
# Restarts the CDAP SDK
# returns: exit code of stop/start, or zero if successful
#
cdap_sdk_restart() { cdap_sdk_stop ; cdap_sdk_start ${@}; };

#
# cdap_sdk_stop
#
cdap_sdk_stop() { cdap_stop_pidfile ${__pidfile} "CDAP Sandbox"; };

#
# cdap_sdk_check_before_start
#
cdap_sdk_check_before_start() {
  cdap_check_pidfile ${__pidfile} Sandbox || return ${?}
  cdap_check_node_version ${CDAP_NODE_VERSION_MINIMUM:-v10.16.2} || return ${?}
  local __node_pid=$(ps | grep ${CDAP_UI_PATH:-ui/server_dist/index.js} | grep -v grep | awk '{ print $1 }')

  if [[ -z ${__node_pid} ]]; then
    : # continue
  else
    # Yeah, this is dangerous, but hey... why not?
    kill -9 ${__node_pid} 2>&1 >/dev/null
  fi
  return 0
}

#
# cdap_sdk_start <foreground> <debug> [port] [args]
#
cdap_sdk_start() {
  local readonly __foreground=${1} __debug=${2} __port=${3}
  shift; shift; shift
  local readonly __args=${@}
  local readonly __ret __pid

  # Default JVM_OPTS for CDAP Sandbox (use larger heap for /opt/cdap Sandbox installs)
  if [[ ${CDAP_HOME} == /opt/cdap ]] || [[ ${CDAP_HOME} == /opt/cdap/sandbox* ]]; then
    CDAP_SDK_DEFAULT_JVM_OPTS="-Xmx3072m"
  else
    CDAP_SDK_DEFAULT_JVM_OPTS="-Xmx2048m"
  fi

  SDK_GC_OPTS="-verbose:gc -Xloggc:${LOG_DIR}/gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=1M"
  if [[ ${HEAPDUMP_ON_OOM} == true ]]; then
    CDAP_SDK_OPTS+=" -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${LOG_DIR}"
  fi

  eval split_jvm_opts ${CDAP_SDK_DEFAULT_JVM_OPTS} ${CDAP_SDK_OPTS} ${SDK_GC_OPTS} ${JAVA_OPTS}

  cdap_sdk_check_before_start || return 1
  cdap_create_local_dir || die "Failed to create LOCAL_DIR: ${LOCAL_DIR}"
  cdap_create_log_dir || die "Failed to create LOG_DIR: ${LOG_DIR}"
  cdap_create_pid_dir || die "Failed to create PID_DIR: ${PID_DIR}"

  rotate_log "${LOG_DIR}"/cdap.log
  rotate_log "${LOG_DIR}"/cdap-debug.log

  # Docker check and warning(s)
  if test -e /proc/1/cgroup && grep docker /proc/1/cgroup 2>&1 >/dev/null; then
    if ${__foreground}; then
      echo "[INFO] Docker detected: running in the foreground with output to STDOUT"
    else
      echo "[WARN] Docker detected, but running in the background! This may fail!"
    fi
    ROUTER_OPTS="-Drouter.address=$(hostname -I | awk '{print $1}')" # -I is safe since we know we're Linux
  fi

  cdap_set_java || die "Unable to locate JAVA or JAVA_HOME"

  # In order to ensure that we can do hacks, need to make sure classpath is sorted
  # so that cdap jars are placed earlier in the classpath than twill or hadoop jars
  CLASSPATH=$(find "${CDAP_HOME}/lib" -type f | sort | tr '\n' ':')
  CLASSPATH="${CLASSPATH}:${CDAP_HOME}/conf/"

  # SDK requires us to be in CDAP_HOME
  cd "${CDAP_HOME}"

  # Start SDK processes
  echo -n "$(date) Starting CDAP Sandbox ..."
  if ${__foreground}; then
    # this eval is needed to get around the double quote issue in KILL_ON_OOM_OPTS
    eval "nice -1 \"${JAVA}\" ${KILL_ON_OOM_OPTS} ${JVM_OPTS[@]} ${ROUTER_OPTS} -classpath \"${CLASSPATH}\" io.cdap.cdap.StandaloneMain | tee -a \"${LOG_DIR}\"/cdap.log"
    __ret=${?}
    return ${__ret}
  else
    # this eval is needed to get around the double quote issue in KILL_ON_OOM_OPTS
    eval "nohup nice -1 \"${JAVA}\" ${KILL_ON_OOM_OPTS} ${JVM_OPTS[@]} ${ROUTER_OPTS} -classpath \"${CLASSPATH}\" io.cdap.cdap.StandaloneMain </dev/null >>\"${LOG_DIR}\"/cdap.log 2>&1 &"
    __ret=${?}
    __pid=${!}
    sleep 2 # wait for JVM spin up
    while kill -0 ${__pid} >/dev/null 2>&1; do
      if grep '..* started successfully' "${LOG_DIR}"/cdap.log >/dev/null 2>&1; then
        if ${__debug}; then
          echo; echo; echo "Remote debugger agent started on port ${__port}"
        else
          echo; echo
        fi
        grep -A 1 '..* started successfully' "${LOG_DIR}"/cdap.log
        break
      elif grep 'Failed to start server' "${LOG_DIR}"/cdap.log >/dev/null 2>&1; then
        echo; echo "Failed to start server"
        stop
        break
      else
        echo -n .
        sleep 1
      fi
    done
    echo
    if ! kill -0 ${__pid} >/dev/null 2>&1; then
      die "Failed to start, please check logs at ${LOG_DIR} for more information"
    fi
    echo ${__pid} > ${__pidfile}
  fi
  return ${__ret}
}

###
#
# CDAP main functions (old scripts)
#

#
# cdap_auth
# Starts CDAP Auth Server service
#
cdap_auth() {
  local readonly MAIN_CLASS=io.cdap.cdap.security.runtime.AuthenticationServerMain
  local readonly MAIN_CLASS_ARGS=
  local readonly JAVA_HEAP_VAR=AUTH_JAVA_HEAPMAX
  local readonly JAVA_OPTS_VAR=AUTH_JAVA_OPTS
  local AUTH_JAVA_HEAPMAX=${AUTH_JAVA_HEAPMAX:--Xmx1024m}
  local EXTRA_CLASSPATH="${EXTRA_CLASSPATH}:/etc/hbase/conf/"
  cdap_start_java || die "Failed to start CDAP ${CDAP_SERVICE} service"
}

#
# cdap_kafka
# Starts CDAP Kafka service
#
cdap_kafka() {
  local readonly MAIN_CLASS=io.cdap.cdap.kafka.run.KafkaServerMain
  local readonly MAIN_CLASS_ARGS=
  local readonly JAVA_HEAP_VAR=KAFKA_JAVA_HEAPMAX
  local readonly JAVA_OPTS_VAR=KAFKA_JAVA_OPTS
  local KAFKA_JAVA_HEAPMAX=${KAFKA_JAVA_HEAPMAX:--Xmx1024m}
  cdap_start_java || die "Failed to start CDAP ${CDAP_SERVICE} service"
}

#
# cdap_master
# Starts CDAP Master service
#
cdap_master() {
  local readonly MAIN_CLASS=io.cdap.cdap.data.runtime.main.MasterServiceMain
  local readonly MAIN_CLASS_ARGS="start"
  local readonly JAVA_HEAP_VAR=MASTER_JAVA_HEAPMAX
  local readonly JAVA_OPTS_VAR=MASTER_JAVA_OPTS
  local MASTER_JAVA_HEAPMAX=${MASTER_JAVA_HEAPMAX:--Xmx1024m}
  # Assuming update-alternatives convention
  local EXTRA_CLASSPATH="${EXTRA_CLASSPATH}:/etc/hbase/conf/"
  cdap_start_java || die "Failed to start CDAP ${CDAP_SERVICE} service"
}

#
# cdap_router
# Starts CDAP Router service
#
cdap_router() {
  local readonly MAIN_CLASS=io.cdap.cdap.gateway.router.RouterMain
  local readonly MAIN_CLASS_ARGS=
  local readonly JAVA_HEAP_VAR=ROUTER_JAVA_HEAPMAX
  local readonly JAVA_OPTS_VAR=ROUTER_JAVA_OPTS
  local ROUTER_JAVA_HEAPMAX=${ROUTER_JAVA_HEAPMAX:--Xmx1024m}
  cdap_start_java || die "Failed to start CDAP ${CDAP_SERVICE} service"
}

#
# cdap_ui
# Starts CDAP UI service
#
cdap_ui() {
  local MAIN_CMD=node
  # Check for embedded node binary, and ensure it's the correct binary ABI for this system
  if test -x "${CDAP_HOME}"/ui/bin/node ; then
    "${CDAP_HOME}"/ui/bin/node --version >/dev/null 2>&1
    if [ $? -eq 0 ] ; then
      MAIN_CMD="${CDAP_HOME}"/ui/bin/node
    elif [[ $(which node 2>/dev/null) ]]; then
      MAIN_CMD=node
      cdap_check_node_version ${CDAP_NODE_VERSION_MINIMUM:-v10.16.2} || return ${?}
    else
      die "Unable to locate Node.js binary (node), is it installed and in the PATH?"
    fi
  else
    cdap_check_node_version ${CDAP_NODE_VERSION_MINIMUM:-v10.16.2} || return ${?}
  fi
  local readonly MAIN_CMD=${MAIN_CMD}
  export NODE_ENV="production"
  local readonly MAIN_CMD_ARGS="${CDAP_HOME}"/${CDAP_UI_PATH:-ui/server_dist/index.js}
  cdap_start_bin || die "Failed to start CDAP ${CDAP_SERVICE} service"
}

#
# cdap_cli [arguments]
# Runs CDAP CLI with the given options, or starts an interactive shell
#
cdap_cli() {
  local readonly __path __libexec __lib __version __script="$(basename ${0}):cdap_cli"
  local readonly __class="io.cdap.cdap.cli.CLIMain"
  cdap_set_java || die "Unable to locate JAVA or JAVA_HOME"
  __path=${CDAP_HOME}
  if [[ -d ${__path}/cli/lib ]]; then
    __libexec=${__path}/cli/libexec
    __lib=${__path}/cli/lib
    __version=$(cdap_version cli)
  else
    __libexec=${__path}/libexec
    __lib=${__path}/lib
    __version=$(cdap_version)
  fi
  CLI_CP=${__libexec}/io.cdap.cdap.cdap-cli-${__version}.jar
  CLI_CP+=:${__lib}/io.cdap.cdap.cdap-cli-${__version}.jar
  if [[ ${CLASSPATH} == '' ]]; then
    CLASSPATH=${CLI_CP}
  else
    CLASSPATH=${CLASSPATH}:${CLI_CP}
  fi
  if [[ -d ${CDAP_CONF} ]]; then
    CLASSPATH=${CLASSPATH}:"${CDAP_CONF}"
  elif [[ -d ${__path}/conf ]]; then
    CLASSPATH=${CLASSPATH}:"${__path}"/conf/
  fi
  "${JAVA}" ${JAVA_OPTS} -cp "${CLASSPATH}" -Dscript=${__script} ${__class} "${@}"
}

#
# cdap_config_tool [arguments]
#
cdap_config_tool() {
  local readonly __path __libexec __lib __script="$(basename ${0}):cdap_config_tool"
  local readonly __authfile="${HOME}"/.cdap.accesstoken.${HOSTNAME}
  local readonly __ret __class=io.cdap.cdap.ui.ConfigurationJsonTool
  cdap_set_java || die "Unable to locate JAVA or JAVA_HOME"
  __path=${CDAP_HOME}
  if [[ -d ${__path}/ui/lib ]]; then
    __libexec=${__path}/ui/libexec
    __lib=${__path}/ui/lib
  else
    __libexec=${__path}/libexec
    __lib=${__path}/lib
  fi
  if [[ ${CLASSPATH} == "" ]]; then
    CLASSPATH=${__lib}/*
  else
    CLASSPATH=${CLASSPATH}:${__lib}/*
  fi
  if [[ -d ${CDAP_CONF} ]]; then
    CLASSPATH=${CLASSPATH}:"${CDAP_CONF}"
  elif [[ -d ${__path}/conf ]]; then
    CLASSPATH=${CLASSPATH}:"${__path}"/conf/
  fi
  # add token file arg with default token file if one is not provided
  local __has_arg=0 __var
  for __var in ${@}; do
    if [[ ${__var} == "--token-file" ]]; then
      __has_arg=1
    fi
  done
  if [[ ${__has_arg} -eq 0 ]] && [[ -f ${__auth_file} ]]; then
    set -- ${@} "--token-file" "${__auth_file}"
  fi

  "${JAVA}" -cp ${CLASSPATH} -Dscript=${__script} ${__class} ${@}
  __ret=${?}
  return ${__ret}
}

#
# cdap_upgrade_tool [arguments]
#
cdap_upgrade_tool() {
  local readonly __ret __class=io.cdap.cdap.data.tools.UpgradeTool

  # check arguments
  if [[ ${1} == 'hbase' ]]; then
    shift
    set -- "upgrade_hbase" ${@}
  else
    set -- "upgrade" ${@}
  fi

  cdap_run_class ${__class} ${@}
  __ret=${?}
  return ${__ret}
}

#
# cdap_apply_pack [arguments]
#
cdap_apply_pack() {
  local __ui_pack=${1}
  local __ext=${__ui_pack##*.}

  cdap_check_node_version ${CDAP_NODE_VERSION_MINIMUM:-v10.16.2} || return ${?}

  if [[ -f ${__ui_pack} ]] && [[ -r ${__ui_pack} ]] && [[ ${__ext} == zip ]]; then
    # ui upgrade script must be run from subdirectory
    cd ${CDAP_HOME}/ui/cdap-ui-upgrade

    cdap_run_bin "npm" "run" "upgrade" "--" "--new-ui-zip-path=${__ui_pack}"
    __ret=${?}
    return ${__ret}
  else
    die "UI pack must be an absolute path to a zip file"
  fi
}

#
# cdap_setup [component] [arguments]
#
cdap_setup() {
  local __component=${1}
  shift

  # currently, only ever need to setup coprocessors
  # in the future, we might want to add more commands, like 'smoketest'
  if [[ ${__component} != 'coprocessors' ]]; then
    die "Setup component must be 'coprocessors'"
  fi

  cdap_setup_${__component} ${@}
  return $?
}

#
# cdap_setup_coprocessors [arguments]
#
cdap_setup_coprocessors() {
  local readonly __ret __class=io.cdap.cdap.data.tools.CoprocessorBuildTool

  cdap_run_class ${__class} check ${@}
  __ret=${?}
  return ${__ret}
}

# cdap_tx_debugger
cdap_tx_debugger() {
  local readonly __path __libexec __lib __script="$(basename ${0}):cdap_tx_debugger"
  local readonly __authfile="${HOME}"/.cdap.accesstoken.${HOSTNAME}
  local readonly __ret __class=io.cdap.cdap.data2.transaction.TransactionManagerDebuggerMain
  cdap_set_java || die "Unable to locate JAVA or JAVA_HOME"
  __path=${CDAP_HOME}
  if [[ -d ${__path}/master/libexec ]]; then
    __libexec=${__path}/master/libexec
    __lib=${__path}/master/lib
  else
    __libexec=${__path}/libexec
    __lib=${__path}/lib
  fi
  if [[ ${CLASSPATH} == "" ]]; then
    CLASSPATH=${__lib}/*
  else
    CLASSPATH=${CLASSPATH}:${__lib}/*
  fi
  if [[ -d ${CDAP_CONF} ]]; then
    CLASSPATH=${CLASSPATH}:"${CDAP_CONF}"
  elif [[ -d ${__path}/conf ]]; then
    CLASSPATH=${CLASSPATH}:"${__path}"/conf/
  fi
  # add token file arg with default token file if one is not provided
  local __has_arg=0 __var
  for __var in ${@}; do
    if [[ ${__var} == "--token-file" ]]; then
      __has_arg=1
    fi
  done
  if [[ ${__has_arg} -eq 0 ]] && [[ -f ${__auth_file} ]]; then
    set -- ${@} "--token-file" "${__auth_file}"
  fi

  "${JAVA}" -cp ${CLASSPATH} -Dscript=${__script} ${__class} ${@}
  __ret=${?}
  return ${__ret}
}

#
# cdap_debug <entity> [arguments]
#
cdap_debug() {
  local readonly __entity=${1}
  shift
  local readonly __ret __args=${@}
  case ${__entity} in
    transactions) cdap_tx_debugger ${__args}; __ret=${?} ;;
    *) echo "Usage: ${0} debug transactions [arguments]"; __ret=1
  esac
  return ${__ret}
}

# Runs CDAP SDK with the given options
cdap_sdk() {
  local readonly __action=${1}
  local readonly PID_DIR=/var/tmp
  local readonly LOG_DIR="${CDAP_HOME}"/logs
  local readonly LOCAL_DIR="${CDAP_HOME}"/data
  local readonly __pidfile=${PID_DIR}/cdap-sdk-${IDENT_STRING}.pid
  case ${__action} in
    start|restart)
      local readonly __command="cdap_sdk_${1}"
      shift
      local __debug=false __foreground=false __port=5005 __arg
      # Parse arguments
      while [[ ${#} -gt 0 ]]; do
        case ${1} in
          --enable-debug)
            shift
            __debug=true
            __arg=${1}
            if [[ ${__arg} =~ ^- ]]; then
              continue
            else
              shift
            fi
            ;;
          --foreground) shift; __foreground=true ;;
          *) __arg=${@}; break ;;
        esac
      done
      # Handle DEBUG
      if ${__debug}; then
        shopt -s extglob
        if [[ ${__arg} =~ ^- ]] || [[ -z ${__arg} ]]; then
          # __arg is either another argument or missing
          : # continue
        elif [[ -n ${__arg##+([0-9])} ]]; then
          # assuming __arg is a port assignment
          if [[ ${__arg} -lt 1024 ]] || [[ ${__arg} -gt 65535 ]]; then
            # number is outside allowed port range
            die "Debug port number must be between 1024 and 65535"
          else
            __port=${__arg}
          fi
        fi
        CDAP_SDK_OPTS+=" -agentlib:jdwp=transport=dt_socket,address=localhost:${__port},server=y,suspend=n"
      fi
      # Execute __command
      ${__command} ${__foreground} ${__debug} ${__port} ${__arg}
      __ret=${?}
      ;;
    status) cdap_status_pidfile ${__pidfile} "CDAP Sandbox"; __ret=${?} ;;
    stop) cdap_sdk_stop; __ret=${?} ;;
    usage) cdap_sdk_usage; __ret=${?} ;;
    cleanup) cdap_sdk_cleanup; __ret=${?} ;;
    *) cdap_sdk_usage; __ret=1 ;; # Return non-zero; called incorrectly
  esac
  return ${__ret}
}

#
# User-definable variables

# Set CDAP_HOME
export CDAP_HOME=$(cdap_home)

# Default CDAP_CONF to either:
# /etc/cdap/conf (package default), or
# ${CDAP_HOME}/conf (sandbox default)
if [[ $(cdap_context) == 'sdk' ]]; then
  export CDAP_CONF=${CDAP_CONF:-${CDAP_HOME}/conf}
else
  export CDAP_CONF=${CDAP_CONF:-/etc/cdap/conf}
fi

# Make sure HOSTNAME is in the environment
export HOSTNAME=$(hostname -f)

# The directory serving as the user directory for master
export LOCAL_DIR=${CDAP_LOCAL_DIR:-/var/tmp/cdap}

# Where log files are stored.
export LOG_DIR=${CDAP_LOG_DIR:-/var/log/cdap}

# A string representing this instance of CDAP. $USER by default.
export IDENT_STRING=${USER}

# Extra Java runtime options.
# Below are what we set by default.  May only work with SUN JVM.
# For more on why as well as other possible settings,
# see http://wiki.apache.org/hadoop/PerformanceTuning
export OPTS=${OPTS:-"-XX:+UseG1GC"}

# The directory where PID files are stored. Default: /var/cdap/run
export PID_DIR=${CDAP_PID_DIR:-/var/cdap/run}

# The directory serving as the java.io.tmpdir directory for master
export TEMP_DIR=${CDAP_TEMP_DIR:-/tmp}

# Default SDK options
CDAP_SDK_OPTS="${OPTS} -Djava.security.krb5.realm= -Djava.security.krb5.kdc= -Djava.awt.headless=true"

export HEAPDUMP_ON_OOM=${HEAPDUMP_ON_OOM:-false}

export NICENESS=${NICENESS:-0}
