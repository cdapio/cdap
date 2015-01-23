#!/bin/bash

# run this file to create a zippable ./target
#  directory that should contain everything
#  needed to run the UI in production

export PWD=`pwd`
export UI_PATH=${0%/*/*}
export DEFAULT_TARGET_PATH="${UI_PATH}/target"
export TARGET_PATH=${1-$DEFAULT_TARGET_PATH}

read -p "Blow away ${TARGET_PATH} and build in it (y/n)? " choice
case "$choice" in
  y|Y ) echo "building...";;
  * ) exit 0;
esac

# delete and recreate target path
[ -d ${TARGET_PATH} ] && rm -rf ${TARGET_PATH}
mkdir ${TARGET_PATH}

touch "${TARGET_PATH}/hello.txt"

echo "Done!"
echo
