#!/usr/bin/env bash

# Build script for Tengo javadocs
# Builds the four modules, copies the HTML files, creates a time-stamped index file
# 
# mvn clean package site -P javadocs -DskipTests -pl tx-core,tx-hbase-compat-0.94,tx-hbase-compat-0.96 -am

SCRIPT_DIR=`pwd`
PARENT_PATH=`cd ../..; pwd`
DATE_STAMP=`date`

APIDOCS="apidocs"
BUILD="build"

TX_API="tx-api"
TX_CORE="tx-core"
TX_HBASE_94="tx-hbase-compat-0.94"
TX_HBASE_96="tx-hbase-compat-0.96"
TX_LIST="$TX_API $TX_CORE $TX_HBASE_94 $TX_HBASE_96"
# Because of dependencies, TX_API gets built by TX_CORE
TX_API_LIST="$TX_CORE,$TX_HBASE_94,$TX_HBASE_96"

INDEX_HTML="index.html"
INDEX_TEMPLATE="index_template.html"
INDEX_DATE_STAMP="DATE_STAMP"

PROJECT="tengo"

ZIP_FILE_NAME="$PROJECT-docs"
ZIP_FILE="$ZIP_FILE_NAME.zip"

STAGING_SERVER="stg-web101.sw.joyent.continuuity.net"
WWW_PATH="/var/www/website-docs/"

function usage() {
  echo "Build script for Project $PROJECT Documentation"
  echo "Usage: $SCRIPT < option >"
  echo ""
  echo "  Options (select one)"
  echo "    build        Clean build of javadocs, HTML docs, copy javadocs into place, zip results"
  echo "    stage        Stages and logins to server"
  echo "  or "
  echo "    docs         Clean build of docs"
  echo "    javadocs     Clean build of javadocs"
  echo "    login        Logs you into $STAGING_SERVER"
  echo "    zip          Zips docs into $ZIP"
#   echo "    sdk          Build SDK"  # Eventually use this for packaging
  echo " "
  exit 1
}

function clean() {
  rm -rf $SCRIPT_DIR/$BUILD
}

function javadocs() {
  clean
  build_javadocs
}

function javadoc_api() {
  # Builds an individual API module
  local api=$1;
  cd $PARENT_PATH;
  target_path="`pwd`/$api/target/site/apidocs";
  mvn clean package site -pl $api -am -Pjavadocs -DskipTests;
  echo "Javadocs for $api are finished. The HTML pages are in $target_path.";
  copy_docs $1 $target_path
}

function javadocs_all() {
  javadoc_api $TX_API
  javadoc_api $TX_CORE
  javadoc_api $TX_HBASE_94
  javadoc_api $TX_HBASE_96
}

function build_javadocs() {
  # Builds all the API modules in one command
  cd $PARENT_PATH;
  target_path="/target/site/apidocs";
  mvn clean package site -P javadocs -DskipTests -pl $TX_API_LIST -am;
  echo "Javadocs for $api are finished. The HTML pages are in $target_path.";
}

function docs() {
  clean
  build_docs
}

function build_docs() {
  echo "Not implemented yet in this branch. Docs are in the other repo."
#   sphinx-build -b html -d build/doctrees source build/html
}

function copy_docs() {
  cd $SCRIPT_DIR
  echo "Creating $BUILD"
  mkdir -p $BUILD
  echo "Copying in $2"
  cp -r $2 $BUILD
  echo "Moving $APIDOCS to $1"
  mv $SCRIPT_DIR/$BUILD/$APIDOCS $SCRIPT_DIR/$BUILD/$1
}

function copy_docs_all() {
  for api in $TX_LIST
  do
    cd $PARENT_PATH;
    target_path="`pwd`/$api/target/site/apidocs";
    copy_docs $api $target_path
  done
}

function create_index_html() {
  sed "s/$INDEX_DATE_STAMP/$DATE_STAMP/g" "$SCRIPT_DIR/$INDEX_TEMPLATE" > "$SCRIPT_DIR/$BUILD/$INDEX_HTML"
}

function make_zip() {
  cd $SCRIPT_DIR/$BUILD
  zip -r $ZIP_FILE_NAME */*
  zip -r $ZIP_FILE_NAME $INDEX_HTML
}

function build() {
  clean
  build_docs
  build_javadocs
  copy_docs_all
  create_index_html
  make_zip
}

function stage_docs() {
  echo "Deploying..."
  echo "rsync -vz $SCRIPT_PATH/$BUILD/$ZIP_FILE \"$USER@$STAGING_SERVER:$ZIP_FILE\""
#   rsync -vz $SCRIPT_PATH/$BUILD/$ZIP_FILE "$USER@$STAGING_SERVER:$ZIP_FILE"
  cd_cmd="cd $WWW_PATH; ls"
  remove_cmd="sudo rm -rf $PROJECT"
  unzip_cmd="sudo unzip ~/$ZIP_FILE; sudo mv $ZIP_FILE_NAME $PROJECT"
  echo ""
  echo "To install on server:"
  echo ""
  echo "  $cd_cmd"
  echo "  $remove_cmd; ls"
  echo "  $unzip_cmd; ls"
  echo ""
  echo "or, on one line:"
  echo ""
  echo "  $cd_cmd; $remove_cmd; ls; $unzip_cmd; ls"
  echo ""
#   login_staging_server
}

function login_staging_server() {
  echo "Logging into:"
  echo "ssh \"$USER@$STAGING_SERVER\""
  ssh "$USER@$STAGING_SERVER"
}

if [ $# -lt 1 ]; then
  usage
  exit 1
fi

case "$1" in
  build )             build; exit 1;;
  docs )              docs; exit 1;;
  build-docs )        build_docs; exit 1;;
  javadocs )          javadocs; exit 1;;
  build-javadocs )    build_javadocs; exit 1;;
  copy_docs_all )     copy_docs_all; exit 1;;
  create_index_html ) create_index_html; exit 1;;
  javadocs )          build_javadocs; exit 1;;
  login )             login_staging_server; exit 1;;
  stage )             stage_docs; exit 1;;
  zip )               make_zip; exit 1;;
  * )                 usage; exit 1;;
esac