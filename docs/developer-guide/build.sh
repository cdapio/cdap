#!/usr/bin/env bash

# Copyright 2014 Cask Data, Inc.
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
API="api"
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
PROJECT_JAVADOCS="$PROJECT_PATH/target/site/apidocs"
SDK_JAVADOCS="$PROJECT_PATH/$API/target/site/$APIDOCS"

ZIP_FILE_NAME=$HTML
ZIP="$ZIP_FILE_NAME.zip"

function usage() {
  cd $PROJECT_PATH
  PROJECT_PATH=`pwd`
  echo "Build script for '$PROJECT_CAPS' docs"
  echo "Usage: $SCRIPT < option > [source]"
  echo ""
  echo "  Options (select one)"
  echo "    build         Clean build of javadocs and HTML docs, copy javadocs and PDFs into place, zip results"
  echo ""
  echo "    docs          Clean build of docs"
  echo "    javadocs      Clean build of javadocs (api module only) for SDK and website"
  echo "    javadocs-full Clean build of javadocs for all modules"
  echo "    rest-pdf      Clean build of REST PDF"
  echo "    license-pdfs  Clean build of License Dependency PDFs"
  echo "    zip           Zips docs into $ZIP"
  echo ""
  echo "    depends       Build Site listing dependencies"
  echo "    sdk           Build SDK"
  echo "  with"
  echo "    source        Path to $PROJECT source for javadocs, if not $PROJECT_PATH"
  echo " "
  exit 1
}

function clean() {
  cd $SCRIPT_PATH
  rm -rf $SCRIPT_PATH/$BUILD
}

function build_docs() {
  clean
  cd $SCRIPT_PATH
  sphinx-build -b html -d build/doctrees source build/html
}

function build_javadocs_full() {
  cd $PROJECT_PATH
  mvn clean site -DskipTests
}

function build_javadocs_sdk() {
  cd $PROJECT_PATH
  mvn clean package javadoc:javadoc -pl api -am -DskipTests -P release
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

function make_zip() {
  cd $SCRIPT_PATH/$BUILD
  zip -r $ZIP_FILE_NAME $HTML/*
}

function build() {
  build_docs
  build_javadocs_sdk
  copy_javadocs_sdk
  copy_license_pdfs
  make_zip
}

function build_rest_pdf() {
  cd $SCRIPT_PATH
#   version # version is not needed because the renaming is done by the pom.xml file
  rm -rf $SCRIPT_PATH/$BUILD_PDF
  mkdir $SCRIPT_PATH/$BUILD_PDF
  python $DOC_GEN_PY -g pdf -o $REST_PDF $REST_SOURCE
}

function build_standalone() {
  cd $PROJECT_PATH
  mvn clean package -DskipTests -P examples && mvn package -pl standalone -am -DskipTests -P dist,release
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
  PROJECT_VERSION=`mvn help:evaluate -o -Dexpression=project.version | grep -v '^\['`
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
