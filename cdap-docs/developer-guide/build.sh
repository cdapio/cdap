#!/usr/bin/env bash

# Copyright © 2014 Cask Data, Inc.
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
  
# Build script for docs
# Builds the docs (all except javadocs and PDFs) from the .rst source files using Sphinx
# Builds the javadocs and copies them into place
# Zips everything up so it can be staged
# REST PDF is built as a separate target and checked in, as it is only used in SDK and not website
# Target for building the SDK
# Targets for both a limited and complete set of javadocs
# Targets not included in usage are intended for internal usage by script

DATE_STAMP=`date`
SCRIPT=`basename $0`

SOURCE="source"
BUILD="build"
BUILD_PDF="build-pdf"
HTML="html"
INCLUDES="_includes"

API="cdap-api"
APIDOCS="apidocs"
JAVADOCS="javadocs"
LICENSES="licenses"
LICENSES_PDF="licenses-pdf"
PROJECT="cdap"
PROJECT_CAPS="CDAP"

SCRIPT_PATH=`pwd`

SOURCE_PATH="$SCRIPT_PATH/$SOURCE"
BUILD_PATH="$SCRIPT_PATH/$BUILD"
HTML_PATH="$BUILD_PATH/$HTML"

DOC_GEN_PY="$SCRIPT_PATH/../tools/doc-gen.py"

REST_SOURCE="$SOURCE_PATH/rest.rst"
REST_PDF="$SCRIPT_PATH/$BUILD_PDF/rest.pdf"

if [ "x$2" == "x" ]; then
  PROJECT_PATH="$SCRIPT_PATH/../../"
else
  PROJECT_PATH="$2"
fi
# PROJECT_JAVADOCS="$PROJECT_PATH/target/site/apidocs"
SDK_JAVADOCS="$PROJECT_PATH/$API/target/site/$APIDOCS"

ZIP_FILE_NAME=$HTML
ZIP="$ZIP_FILE_NAME.zip"

# Set Google Analytics Codes
# Corporate Docs Code
GOOGLE_ANALYTICS_WEB="UA-55077523-3"
WEB="web"
# CDAP Project Code
GOOGLE_ANALYTICS_GITHUB="UA-55081520-2"
GITHUB="github"

function usage() {
  cd $PROJECT_PATH
  PROJECT_PATH=`pwd`
  echo "Build script for '$PROJECT_CAPS' docs"
  echo "Usage: $SCRIPT < option > [source]"
  echo ""
  echo "  Options (select one)"
  echo "    build          Clean build of javadocs and HTML docs, copy javadocs and PDFs into place, zip results"
  echo "    build-includes Clean conversion of linked markdown to _includes directory reST files"
  echo "    build-github   Clean build and zip for placing on GitHub"
  echo "    build-web      Clean build and zip for placing on docs.cask.co webserver"
  echo ""
  echo "    docs           Clean build of docs"
  echo "    javadocs       Clean build of javadocs ($API module only) for SDK and website"
  echo "    javadocs-full  Clean build of javadocs for all modules"
  echo "    rest-pdf       Clean build of REST PDF"
  echo "    license-pdfs   Clean build of License Dependency PDFs"
  echo "    zip            Zips docs into $ZIP"
  echo ""
  echo "    check-includes Check if included files have changed from source"
  echo "    depends        Build Site listing dependencies"
  echo "    sdk            Build SDK"
  echo "  with"
  echo "    source         Path to $PROJECT source for javadocs, if not $PROJECT_PATH"
  echo " "
  exit 1
}

function clean() {
  cd $SCRIPT_PATH
  rm -rf $SCRIPT_PATH/$BUILD
  mkdir $SCRIPT_PATH/$BUILD
}

function build_docs() {
  clean
  cd $SCRIPT_PATH
  check_includes
  sphinx-build -b html -d build/doctrees source build/html
}

function build_docs_google() {
  clean
  cd $SCRIPT_PATH
  check_includes
  sphinx-build -D googleanalytics_id=$1 -D googleanalytics_enabled=1 -b html -d build/doctrees source build/html
}

function build_javadocs_full() {
  cd $PROJECT_PATH
  mvn clean site -DskipTests
}

function build_javadocs_sdk() {
  cd $PROJECT_PATH
  MAVEN_OPTS="-Xmx512m"  mvn clean package javadoc:javadoc -pl $API -am -DskipTests -P release
}

function copy_javadocs_sdk() {
  cd $BUILD_PATH/$HTML
  rm -rf $JAVADOCS
  cp -r $SDK_JAVADOCS .
  mv -f $APIDOCS $JAVADOCS
}

function build_license_pdfs() {
  cd $SCRIPT_PATH
  rm -rf $SCRIPT_PATH/$LICENSES_PDF
  mkdir $SCRIPT_PATH/$LICENSES_PDF
  E_DEP="cdap-enterprise-dependencies"
  L_DEP="cdap-level-1-dependencies"
  S_DEP="cdap-standalone-dependencies"
  # paths are relative to location of $DOC_GEN script
  LIC_PDF="../../../developer-guide/$LICENSES_PDF"
  LIC_RST="../developer-guide/source/$LICENSES"
  echo ""
  echo "Building $E_DEP"
  python $DOC_GEN_PY -g pdf -o $LIC_PDF/$E_DEP.pdf $LIC_RST/$E_DEP.rst
  echo ""
  echo "Building $L_DEP"
  python $DOC_GEN_PY -g pdf -o $LIC_PDF/$L_DEP.pdf $LIC_RST/$L_DEP.rst
  echo ""
  echo "Building $S_DEP"
  python $DOC_GEN_PY -g pdf -o $LIC_PDF/$S_DEP.pdf $LIC_RST/$S_DEP.rst
}

function copy_license_pdfs() {
  cd $BUILD_PATH/$HTML/$LICENSES
  cp $SCRIPT_PATH/$LICENSES_PDF/* .
}

function make_zip_html() {
  version
  ZIP_FILE_NAME="$PROJECT-docs-$PROJECT_VERSION.zip"
  cd $SCRIPT_PATH/$BUILD
  zip -r $ZIP_FILE_NAME $HTML/*
}

function make_zip() {
# This creates a zip that unpacks to the same name
  version
  ZIP_DIR_NAME="$PROJECT-docs-$PROJECT_VERSION-$1"
  cd $SCRIPT_PATH/$BUILD
  mv $HTML $ZIP_DIR_NAME
  zip -r $ZIP_DIR_NAME.zip $ZIP_DIR_NAME/*
}

function make_zip_localized() {
# This creates a named zip that unpacks to the Project Version, localized to english
  version
  ZIP_DIR_NAME="$PROJECT-docs-$PROJECT_VERSION-$1"
  cd $SCRIPT_PATH/$BUILD
  mkdir $PROJECT_VERSION
  mv $HTML $PROJECT_VERSION/en
  zip -r $ZIP_DIR_NAME.zip $PROJECT_VERSION/*
}

function build() {
  build_docs
  build_javadocs_sdk
  copy_javadocs_sdk
  copy_license_pdfs
  make_zip
}

function build_web() {
# This is used to stage files at cdap-integration10031-1000.dev.continuuity.net
# desired path is 2.5.0-SNAPSHOT/en/*
  build_docs_google $GOOGLE_ANALYTICS_WEB
  build_javadocs_sdk
  copy_javadocs_sdk
  copy_license_pdfs
  make_zip_localized $WEB
}

function build_github() {
  # GitHub requires a .nojekyll file at the root to allow for Sphinx's directories beginning with underscores
  build_docs_google $GOOGLE_ANALYTICS_GITHUB
  build_javadocs_sdk
  copy_javadocs_sdk
  copy_license_pdfs
  make_zip $GITHUB
  ZIP_DIR_NAME="$PROJECT-docs-$PROJECT_VERSION-$GITHUB"
  cd $SCRIPT_PATH/$BUILD
  touch $ZIP_DIR_NAME/.nojekyll
  zip $ZIP_DIR_NAME.zip $ZIP_DIR_NAME/.nojekyll
}

function build_rest_pdf() {
  cd $SCRIPT_PATH
#   version # version is not needed because the renaming is done by the pom.xml file
  rm -rf $SCRIPT_PATH/$BUILD_PDF
  mkdir $SCRIPT_PATH/$BUILD_PDF
  python $DOC_GEN_PY -g pdf -o $REST_PDF $REST_SOURCE
}

function check_includes() {
  if hash pandoc 2>/dev/null; then
    echo "pandoc is installed; checking the README includes."
    # Build includes
    BUILD_INCLUDES_DIR=$SCRIPT_PATH/$BUILD/$INCLUDES
    rm -rf $BUILD_INCLUDES_DIR
    mkdir $BUILD_INCLUDES_DIR
    pandoc_includes $BUILD_INCLUDES_DIR
    # Test included files
    test_include cdap-authentication-clients-java.rst
    test_include cdap-authentication-clients-python.rst
    test_include cdap-file-drop-zone.rst
    test_include cdap-file-tailer.rst
    test_include cdap-flume.rst
    test_include cdap-stream-clients-java.rst
    test_include cdap-stream-clients-python.rst
  else
    echo "WARNING: pandoc not installed; checked-in README includes will be used instead."
  fi
}

function test_include() {
  BUILD_INCLUDES_DIR=$SCRIPT_PATH/$BUILD/$INCLUDES
  SOURCE_INCLUDES_DIR=$SCRIPT_PATH/$SOURCE/$INCLUDES
  EXAMPLE=$1
  if diff -q $BUILD_INCLUDES_DIR/$1 $SOURCE_INCLUDES_DIR/$1 2>/dev/null; then
    echo "Tested $1; matches checked-in include file."
  else
    echo "WARNING: Tested $1; does not match checked-in include file. Copying to source directory."
    cp -f $BUILD_INCLUDES_DIR/$1 $SOURCE_INCLUDES_DIR/$1
  fi
}

function build_includes() {
  if hash pandoc 2>/dev/null; then
    echo "pandoc is installed; rebuilding the README includes."
    SOURCE_INCLUDES_DIR=$SCRIPT_PATH/$SOURCE/$INCLUDES
    rm -rf $SOURCE_INCLUDES_DIR
    mkdir $SOURCE_INCLUDES_DIR
    pandoc_includes $SOURCE_INCLUDES_DIR
  else
    echo "WARNING: pandoc not installed; checked-in README includes will be used instead."
  fi
}

function pandoc_includes() {
  # Uses pandoc to translate the README markdown files to rst in the target directory
  INCLUDES_DIR=$1
  TYPE="local"
#   if [ $TYPE="local" ]; then
#   #   authentication-client java
#     pandoc -t rst -r markdown ../../../cdap-clients/cdap-authentication-clients/java/README.md  -o $INCLUDES_DIR/cdap-authentication-clients-java.rst
#   #   authentication-client python
#     pandoc -t rst -r markdown ../../../cdap-clients/cdap-authentication-clients/python/README.md  -o $INCLUDES_DIR/cdap-authentication-clients-python.rst
#   #   file-drop-zone
#     pandoc -t rst -r markdown ../../../cdap-ingest/cdap-file-drop-zone/README.md  -o $INCLUDES_DIR/cdap-file-drop-zone.rst
#   #   file-tailer
#     pandoc -t rst -r markdown ../../../cdap-ingest/cdap-file-tailer/README.md  -o $INCLUDES_DIR/cdap-file-tailer.rst
#   #   flume
#     pandoc -t rst -r markdown ../../../cdap-ingest/cdap-flume/README.md  -o $INCLUDES_DIR/cdap-flume.rst
#   #   stream-client java
#     pandoc -t rst -r markdown ../../../cdap-ingest/cdap-stream-clients/java/README.md  -o $INCLUDES_DIR/cdap-stream-clients-java.rst
#   #   stream-client python
#     pandoc -t rst -r markdown ../../../cdap-ingest/cdap-stream-clients/python/README.md  -o $INCLUDES_DIR/cdap-stream-clients-python.rst
#   else
#   # Remote
#   GITHUB="https://raw.githubusercontent.com/caskdata"
#   #   authentication-client java
#     pandoc -t rst -r markdown $GITHUB/cdap-clients/$VERSION/cdap-authentication-clients/java/README.md  -o $INCLUDES_DIR/cdap-authentication-clients-java.rst
#   #   authentication-client python
#     pandoc -t rst -r markdown $GITHUB/cdap-clients/$VERSION/cdap-authentication-clients/python/README.md  -o $INCLUDES_DIR/cdap-authentication-clients-python.rst
#   #   file-drop-zone
#     pandoc -t rst -r markdown $GITHUB/cdap-ingest/$VERSION/cdap-file-drop-zone/README.md  -o $INCLUDES_DIR/cdap-file-drop-zone.rst
#   #   file-tailer
#     pandoc -t rst -r markdown $GITHUB/cdap-ingest/$VERSION/cdap-file-tailer/README.md  -o $INCLUDES_DIR/cdap-file-tailer.rst
#   #   flume
#     pandoc -t rst -r markdown $GITHUB/cdap-ingest/$VERSION/cdap-flume/README.md  -o $INCLUDES_DIR/cdap-flume.rst
#   #   stream-client java
#     pandoc -t rst -r markdown $GITHUB/cdap-ingest/$VERSION/cdap-stream-clients/java/README.md  -o $INCLUDES_DIR/cdap-stream-clients-java.rst
#   #   stream-client python
#     pandoc -t rst -r markdown $GITHUB/cdap-ingest/$VERSION/cdap-stream-clients/python/README.md  -o $INCLUDES_DIR/cdap-stream-clients-python.rst
#   fi
  
  if [ $TYPE="local" ]; then
    MD_CLIENTS="../../../cdap-clients"
    MD_INGEST="../../../cdap-ingest"
  else
    GITHUB="https://raw.githubusercontent.com/caskdata"
    VERSION="release/1.0.0" # for development
    #   VERSION="v1.0.1"
    MD_CLIENTS="$GITHUB/cdap-clients/$VERSION"
    MD_INGEST="$GITHUB/cdap-ingest/$VERSION"
  fi
    
  #   authentication-client java
  pandoc -t rst -r markdown $MD_CLIENTS/cdap-authentication-clients/java/README.md  -o $INCLUDES_DIR/cdap-authentication-clients-java.rst
  #   authentication-client python
  pandoc -t rst -r markdown $MD_CLIENTS/cdap-authentication-clients/python/README.md  -o $INCLUDES_DIR/cdap-authentication-clients-python.rst
  #   file-drop-zone
  pandoc -t rst -r markdown $MD_INGEST/cdap-file-drop-zone/README.md  -o $INCLUDES_DIR/cdap-file-drop-zone.rst
  #   file-tailer
  pandoc -t rst -r markdown $MD_INGEST/cdap-file-tailer/README.md  -o $INCLUDES_DIR/cdap-file-tailer.rst
  #   flume
  pandoc -t rst -r markdown $MD_INGEST/cdap-flume/README.md  -o $INCLUDES_DIR/cdap-flume.rst
  #   stream-client java
  pandoc -t rst -r markdown $MD_INGEST/cdap-stream-clients/java/README.md  -o $INCLUDES_DIR/cdap-stream-clients-java.rst
  #   stream-client python
  pandoc -t rst -r markdown $MD_INGEST/cdap-stream-clients/python/README.md  -o $INCLUDES_DIR/cdap-stream-clients-python.rst
 
}

function build_standalone() {
  cd $PROJECT_PATH
  MAVEN_OPTS="-Xmx512m" mvn clean package -DskipTests -P examples && mvn package -pl standalone -am -DskipTests -P dist,release
}

function build_sdk() {
  build_rest_pdf
  build_standalone
}

function build_dependencies() {
  cd $PROJECT_PATH
  mvn clean package site -am -Pjavadocs -DskipTests
}

function version() {
  cd $PROJECT_PATH
#   PROJECT_VERSION=`mvn help:evaluate -o -Dexpression=project.version | grep -v '^\['`
#   PROJECT_VERSION="2.5.0"
  PROJECT_VERSION=`grep "<version>" pom.xml`
  PROJECT_VERSION=${PROJECT_VERSION#*<version>}
  PROJECT_VERSION=${PROJECT_VERSION%%</version>*}
  IFS=/ read -a branch <<< "`git rev-parse --abbrev-ref HEAD`"
  GIT_BRANCH="${branch[1]}"
}

function print_version() {
  version
  echo "PROJECT_PATH: $PROJECT_PATH"
  echo "PROJECT_VERSION: $PROJECT_VERSION"
  echo "GIT_BRANCH: $GIT_BRANCH"
}

function test() {
  echo "Test..."
  echo "Version..."
  print_version
  echo "Build all docs..."
  build
  echo "Build SDK..."
  build_sdk
  echo "Test completed."
}

if [ $# -lt 1 ]; then
  usage
  exit 1
fi

case "$1" in
  build )             build; exit 1;;
  build-includes )    build_includes; exit 1;;
  build-github )      build_github; exit 1;;
  build-web )         build_web; exit 1;;
  check-includes )    check_includes; exit 1;;
  docs )              build_docs; exit 1;;
  license-pdfs )      build_license_pdfs; exit 1;;
  build-standalone )  build_standalone; exit 1;;
  copy-javadocs )     copy_javadocs; exit 1;;
  copy-license-pdfs ) copy_license_pdfs; exit 1;;
  javadocs )          build_javadocs_sdk; exit 1;;
  javadocs-full )     build_javadocs_full; exit 1;;
  depends )           build_dependencies; exit 1;;
  rest-pdf )          build_rest_pdf; exit 1;;
  sdk )               build_sdk; exit 1;;
  version )           print_version; exit 1;;
  test )              test; exit 1;;
  zip )               make_zip; exit 1;;
  * )                 usage; exit 1;;
esac