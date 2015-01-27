#!/bin/bash

# run this file to create a zippable "target"
#  directory that will contain everything
#  needed to run the UI in production

UI_PATH=$(cd $(dirname ${BASH_SOURCE[0]})/.. && pwd)
TARGET_PATH=${1:-${UI_PATH}/target}

die ( ) { echo ; echo "ERROR: Failed while ${*}" ; echo ; exit 1; }

read -p "Blow away ${TARGET_PATH} and build in it (y/n)? " choice
case ${choice} in
  y|Y ) echo "OK, building CDAP 3.0 user interface!";;
  * ) echo "You must enter Y or y to build!" && exit 0;
esac

echo "┌─────────────────────────────────────────────────────────────────"
echo "├┄ deleting and recreating target path..."
[ -d ${TARGET_PATH} ] && rm -rf ${TARGET_PATH}
mkdir -p ${TARGET_PATH}

echo "├┄ ensuring global dependencies are present..."
npm install -g bower gulp &>/dev/null || die "installing global dependencies"

echo "├┄ cleaning house in ${UI_PATH}..."
cd ${UI_PATH}
[ -d ${UI_PATH}/node_modules ] && rm -rf ${UI_PATH}/node_modules
[ -d ${UI_PATH}/bower_components ] && rm -rf ${UI_PATH}/bower_components

echo "├┄ fetching dependencies... it may take a while..."
npm install &>/dev/null || die "running \"npm install\""
bower install --config.interactive=false &>/dev/null || die "running \"bower install\""

echo "├┄ making a fresh dist..."
gulp clean &>/dev/null || die "running \"gulp clean\""
gulp distribute &>/dev/null || die "running \"gulp distribute\""

echo "├┄ copying relevant files to the target..."
cp package.json ${TARGET_PATH}
cp bower.json ${TARGET_PATH}
cp server.js ${TARGET_PATH}
cp -r server/ ${TARGET_PATH}/server
cp -r dist/ ${TARGET_PATH}/dist

echo "├┄ installing production npm dependencies..."
cd ${TARGET_PATH}
npm install --production &>/dev/null || die "installing production npm dependencies"

echo "└┄ All done!"
echo
