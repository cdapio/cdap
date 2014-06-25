#!/usr/bin/env bash

# Build script for Reactor docs
# Builds the docs
# Copies the javadocs into place
# Zips everything up so it can be staged

DATE_STAMP=`date`
SCRIPT=`basename $0`

BUILD="build"
HTML="html"
APIDOCS="apidocs"
JAVADOCS="javadocs"

SCRIPT_PATH=`pwd`
BUILD_PATH="$SCRIPT_PATH/$BUILD"
HTML_PATH="$BUILD_PATH/$HTML"

if [ "x$2" == "x" ]; then
  REACTOR_PATH="$SCRIPT_PATH/../../"
else
  REACTOR_PATH="$2"
fi
REACTOR_JAVADOCS="$REACTOR_PATH/continuuity-api/target/site/apidocs"

ZIP_FILE_NAME=$HTML
ZIP_FILE="$ZIP_FILE_NAME.zip"
STAGING_SERVER="stg-web101.sw.joyent.continuuity.net"

function usage() {
  cd $REACTOR_PATH
  REACTOR_PATH=`pwd`
  echo "Build script for Reactor docs"
  echo "Usage: $SCRIPT < option > [reactor]"
  echo ""
  echo "  Options (select one)"
  echo "    build       Clean build of javadocs, docs, copy javadocs, zip results"
  echo "    stage       Stages and logins to server"
  echo "  or "
  echo "    build-docs  Clean build of docs"
  echo "    javadocs    Clean build of javadocs"
  echo "    login       Logs you into $STAGING_SERVER"
  echo "    reactor     Path to Reactor source for javadocs, if not $REACTOR_PATH"
  echo "    zip         Zips docs into $ZIP_FILE"
  echo "  or "
  echo "    depends     Build Site listing dependencies"  
  echo "    sdk         Build SDK"  
  echo " "
  exit 1
}

function clean() {
  rm -rf $SCRIPT_PATH/$BUILD
}

function build_docs() {
  clean
  sphinx-build -b html -d build/doctrees source build/html
}

function copy_javadocs() {
  cd $BUILD_PATH/$HTML
  rm -rf $JAVADOCS
  cp -r $REACTOR_JAVADOCS .
  mv -f $APIDOCS $JAVADOCS
}

function make_zip() {
  cd $SCRIPT_PATH/$BUILD
  zip -r $ZIP_FILE_NAME $HTML/*
}

function stage_docs() {
  echo "Deploying..."
  echo "rsync -vz $SCRIPT_PATH/$BUILD/$ZIP_FILE \"$USER@$STAGING_SERVER:$ZIP_FILE\""
  rsync -vz $SCRIPT_PATH/$BUILD/$ZIP_FILE "$USER@$STAGING_SERVER:$ZIP_FILE"
  echo ""
  echo "To install on server:"
  echo "cd /var/www/reactor"
  echo "sudo rm -rf *"
  echo "sudo unzip ~/$ZIP_FILE; sudo mv $HTML <final-dirname>"
  echo ""
  login_staging_server
}

function login_staging_server() {
  ssh "$USER@$STAGING_SERVER"
}

function javadocs() {
  cd $REACTOR_PATH
  mvn clean package site -pl continuuity-api -am -Pjavadocs -DskipTests
}

function build() {
   clean
   build_docs
   copy_javadocs
   make_zip
}

function build_sdk() {
  cd $REACTOR_PATH
  mvn clean package -DskipTests -P examples && mvn package -pl singlenode -am -DskipTests -P dist,release
}

function build_dependencies() {
  cd $REACTOR_PATH
  mvn clean package site -am -Pjavadocs -DskipTests
}

if [ $# -lt 1 ]; then
  usage
  exit 1
fi

case "$1" in
  build )      build; exit 1;;
  build-docs ) build_docs; exit 1;;
  javadocs )   javadocs; exit 1;;
  depends )    build_dependencies; exit 1;;
  login )      login_staging_server; exit 1;;
  sdk )        build_sdk; exit 1;;
  stage )      stage_docs; exit 1;;
  zip )        make_zip; exit 1;;
  * )          usage; exit 1;;
esac
