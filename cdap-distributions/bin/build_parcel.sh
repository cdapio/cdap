#!/usr/bin/env bash

# Ensure environment is as expected, with maven-produced staging directories
function validate_env {
  # First ensure we are in clean state and no partial parcel build exists
  if [ -d ${STAGE_DIR} ]; then
    echo "Staging directory ${STAGE_DIR} already exists."
    exit 1
  fi

  # Check that all components have been built by maven with the same version
  local __component
  for __component in ${COMPONENTS}; do
    set_and_check_version ${__component}
  done
}

# determine version by looking at $COMPONENT_HOME/target/stage-packaging/opt/cdap/$COMPONENT/VERSION
# and ensuring that all component versions match
function set_and_check_version {
  local __component=$1
  local __component_home="${REPO_HOME}/cdap-${__component}"
  local __version_file="${__component_home}/target/stage-packaging/opt/cdap/${__component}/VERSION"
  if [ -f ${__version_file} ]; then
    local __component_version=`cat ${__version_file}`
    echo "component: ${__component}, version: ${__component_version}"
    # Check that the VERSION file had some content
    if [ -z "${__component_version}" ]; then
      echo "Component ${__component} has an undefined version, expected in ${__version_file}"
      exit 1
    fi
    # If this is the first iteration, set the expected ${VERSION}
    if [ -z "$VERSION" ]; then
      VERSION=${__component_version}
    fi 
    # Ensure that each component has the same version
    if [ "${VERSION}" != "${__component_version}" ] ; then
      echo "Mismatched versions found. Expecting ${VERSION}, found component ${__component} version: ${__component_version}"
      exit 1
    fi
  else
    echo "No version file found for component ${__component}, expecting ${__version_file}"
    exit 1
  fi
}

function stage_artifacts {
  # Create staging directory
  mkdir -p ${STAGE_DIR}/${PARCEL_ROOT_DIR}

  # Copy each built component
  local __component
  for __component in ${COMPONENTS}; do
    local __component_home="${REPO_HOME}/cdap-${__component}"
    cp -fpPR ${__component_home}/target/stage-packaging/opt/cdap/* ${STAGE_DIR}/${PARCEL_ROOT_DIR}/.
  done
}

function stage_parcel_bits {

  # Copy the shared default /etc/conf.dist dir
  mkdir -p ${STAGE_DIR}/${PARCEL_ROOT_DIR}/etc/cdap/conf.dist
  cp -fpPR ${DISTRIBUTIONS_HOME}/src/etc/cdap/conf.dist/* ${STAGE_DIR}/${PARCEL_ROOT_DIR}/etc/cdap/conf.dist/.

  # Copy in the parcel-specific meta dir
  local __meta_dir=${DISTRIBUTIONS_HOME}/src/parcel/meta
  cp -fpPR ${__meta_dir} ${STAGE_DIR}/${PARCEL_ROOT_DIR}/.

  # substitute are version
  sed -i -e "s#{{VERSION}}#${VERSION}#" ${STAGE_DIR}/${PARCEL_ROOT_DIR}/meta/parcel.json

}

function generate_parcel {

  #cd ${TARGET_DIR}
  tar czf ${TARGET_DIR}/${PARCEL_NAME} -C ${STAGE_DIR} ${PARCEL_ROOT_DIR}/ --owner=root --group=root

}

# Parcel name vars 
PARCEL_BASE="CDAP"
PARCEL_SUFFIX="el6"


# Components should map to top-level directories: "cdap-${COMPONENT}"
COMPONENTS="gateway hbase-compat-0.94 hbase-compat-0.96 hbase-compat-0.98 kafka master security web-app"

# Find our script's location and base repo directory
# Resolve links: $0 may be a link
PRG=${0}
# Need this for relative symlinks.
while [ -h ${PRG} ]; do
    ls=`ls -ld ${PRG}`
    link=`expr ${ls} : '.*-> \(.*\)$'`
    if expr ${link} : '/.*' > /dev/null; then
        PRG=${link}
    else
        PRG=`dirname ${PRG}`/${link}
    fi
done
cd `dirname ${PRG}`/.. >&-
DISTRIBUTIONS_HOME=`pwd -P`
cd `dirname ${DISTRIBUTIONS_HOME}` >&-
REPO_HOME=`pwd -P`

TARGET_DIR=${DISTRIBUTIONS_HOME}/target
STAGE_DIR=${TARGET_DIR}/parcel
#mkdir -p ${STAGE_DIR}

validate_env

echo "Using version: ${VERSION}"
PARCEL_ROOT_DIR="${PARCEL_BASE}-${VERSION}"
PARCEL_NAME="${PARCEL_BASE}-${VERSION}-${PARCEL_SUFFIX}.parcel"

stage_artifacts

stage_parcel_bits

generate_parcel

