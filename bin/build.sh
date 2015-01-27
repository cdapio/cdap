#!/bin/bash

# run this file to create a zippable "target"
#  directory that will contain everything
#  needed to run the UI in production

PWD=`pwd`
UI_PATH=$(cd $(dirname ${BASH_SOURCE[0]})/.. && pwd)
TARGET_PATH=${1:-${UI_PATH}/target}

read -p "Blow away ${TARGET_PATH} and build in it (y/n)? " choice
case "$choice" in
  y|Y ) echo "OK, building CDAP 3.0 user interface!";;
  * ) exit 0;
esac

echo "┌─────────────────────────────────────────────────────────────────"
echo "├┄ deleting and recreating target path..."
[ -d ${TARGET_PATH} ] && rm -rf ${TARGET_PATH}
mkdir ${TARGET_PATH}

echo "├┄ ensuring global dependencies are present..."
npm install -g bower gulp &>/dev/null

echo "├┄ cleaning house in ${UI_PATH}..."
cd ${UI_PATH}
[ -d ${UI_PATH}/node_modules ] && rm -rf ${UI_PATH}/node_modules
[ -d ${UI_PATH}/bower_components ] && rm -rf ${UI_PATH}/bower_components

echo "├┄ installing development (npm) dependencies..."
npm install &>/dev/null

echo "├┄ installing client-side (bower) dependencies..."
bower install &>/dev/null

echo "├┄ making a fresh dist..."
gulp clean &>/dev/null
gulp distribute &>/dev/null

echo "├┄ copying files to the target..."
cp package.json ${TARGET_PATH}
cp bower.json ${TARGET_PATH}
cp server.js ${TARGET_PATH}
cp -r server/ ${TARGET_PATH}/server
cp -r dist/ ${TARGET_PATH}/dist

echo "├┄ installing production (npm) dependencies..."
cd ${TARGET_PATH}
npm install --production &>/dev/null

echo "└┄ All done!"
cd ${PWD}
echo
