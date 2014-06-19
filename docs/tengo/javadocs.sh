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
ZIP_FILE="tengo-docs"

function usage() {
  echo "Build script for Tengo javadocs"
  echo "Usage: $script"
}

function clean() {
  rm -rf $SCRIPT_DIR/$BUILD
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

function javadocs() {
  # Builds all the API modules in one command
  cd $PARENT_PATH;
  target_path="/target/site/apidocs";
  mvn clean package site -P javadocs -DskipTests -pl $TX_API_LIST -am;
  echo "Javadocs for $api are finished. The HTML pages are in $target_path.";
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
  zip -r $ZIP_FILE */*
  zip -r $ZIP_FILE $INDEX_HTML
}

clean
javadocs
copy_docs_all
create_index_html
make_zip
