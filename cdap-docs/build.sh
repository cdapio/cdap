#!/usr/bin/env bash

# Copyright © 2014-2015 Cask Data, Inc.
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

# Builds:
#
# admin-manual
# developers-manual
# reference-manual
# examples-manual

# Builds each of these individually, and then packages them into a single zip file for distribution.
# _common directory holds common files and scripts.

source _common/common-build.sh

BUILD_TEMP="build-temp"
COMMON="_common"
COMMON_HTACCESS="$COMMON/htaccess"
COMMON_IMAGES="$COMMON/_images"
COMMON_SOURCE="$COMMON/_source"
COMMON_CONF_PY="$COMMON/common_conf.py"
COMMON_HIGHLEVEL_PY="$COMMON/highlevel_conf.py"
COMMON_PLACEHOLDER="$COMMON/_source/placeholder_index._rst"
HTACCESS="htaccess"

ARG_1="$1"
ARG_2="$2"
ARG_3="$3"

function set_project_path() {
  if [ "x$ARG_2" == "x" ]; then
    PROJECT_PATH="$SCRIPT_PATH/../"
  else
    PROJECT_PATH="$SCRIPT_PATH/../../$ARG_2"
  fi
}

function usage() {
  cd $PROJECT_PATH
  PROJECT_PATH=`pwd`
  echo "Build script for '$PROJECT_CAPS' docs"
  echo "Usage: $SCRIPT < option > [source test_includes]"
  echo ""
  echo "  Options (select one)"
  echo "    all            Clean build of everything: HTML docs and Javadocs, GitHub and Web versions"
  echo ""
  echo "    docs           Clean build of just the HTML docs, skipping Javadocs, zipped for placing on docs.cask.co webserver"
  echo "    docs-github    Clean build of HTML docs and Javadocs, zipped for placing on GitHub"
  echo "    docs-web       Clean build of HTML docs and Javadocs, zipped for placing on docs.cask.co webserver"
  echo ""
  echo "    licenses       Clean build of License Dependency PDFs"
  echo "    sdk            Build SDK"
  echo "    version        Print the version information"
  echo ""
  echo "  with"
  echo "    source         Path to $PROJECT source, if not $PROJECT_PATH"
  echo "    test_includes  local, remote or neither (default: remote); must specify source if used"
  echo ""
}

function run_command() {
  case "$1" in
    all )               build_all; exit 1;;
    docs )              build_docs; exit 1;;
    docs-github )       build_docs_github; exit 1;;
    docs-web )          build_docs_web; exit 1;;
    licenses )          build_license_depends; exit 1;;
    sdk )               build_sdk; exit 1;;
    version )           print_version; exit 1;;
    test )              test; exit 1;;
    * )                 usage; exit 1;;
  esac
}
################################################## new

function clean() {
  cd $SCRIPT_PATH
  rm -rf $SCRIPT_PATH/$BUILD/*
  mkdir -p $SCRIPT_PATH/$BUILD/$HTML
  mkdir -p $SCRIPT_PATH/$BUILD/$SOURCE
  echo "Cleaned $BUILD directory"
  echo ""
}

function copy_source() {
  echo "Copying source for $1 ($2) ..."
  cd $SCRIPT_PATH
  mkdir -p $SCRIPT_PATH/$BUILD/$SOURCE/$1
  rewrite $COMMON_PLACEHOLDER $BUILD/$SOURCE/$1/index.rst "<placeholder>" "$2"
  echo ""
}

function copy_html() {
  echo "Copying html for $1..."
  cd $SCRIPT_PATH
  rm -rf $SCRIPT_PATH/$BUILD/$HTML/$1
  cp -r $1/$BUILD/$HTML $BUILD/$HTML/$1
  echo ""
}

function build_docs_outer_level() {
  echo ""
  echo "========================================================"
  echo "Building outer-level docs..."
  echo "========================================================"
  echo ""
  clean
  version
  
  # Copies placeholder file and renames it
  copy_source admin-manual        "Administration Manual"
  copy_source developers-manual   "Developers’ Manual"
  copy_source integrations-manual "Integrations"
  copy_source reference-manual    "Reference Manual"
  copy_source examples-manual     "Examples, Guides, and Tutorials"

  # Build outer-level docs
  cd $SCRIPT_PATH
  cp $COMMON_HIGHLEVEL_PY  $BUILD/$SOURCE/conf.py
  cp -R $COMMON_IMAGES     $BUILD/$SOURCE/
  cp $COMMON_SOURCE/*.rst  $BUILD/$SOURCE/
  
  if [ "x$1" == "x" ]; then
    sphinx-build -b html -d build/doctrees build/source build/html
  else
    sphinx-build -D googleanalytics_id=$1 -D googleanalytics_enabled=1 -b html -d build/doctrees build/source build/html
  fi
  
  # Copy lower-level doc manuals
  copy_html admin-manual
  copy_html developers-manual
  copy_html integrations-manual
  copy_html reference-manual
  copy_html examples-manual

  local project_dir
  # Rewrite 404 file, using branch if not a release
  if [ "x$GIT_BRANCH_TYPE" == "xfeature" ]; then
    project_dir=$PROJECT_VERSION-$GIT_BRANCH
  else
    project_dir=$PROJECT_VERSION
  fi
  rewrite $BUILD/$HTML/404.html "src=\"_static"  "src=\"/cdap/$project_dir/en/_static"
  rewrite $BUILD/$HTML/404.html "src=\"_images"  "src=\"/cdap/$project_dir/en/_images"
  rewrite $BUILD/$HTML/404.html "/href=\"http/!s|href=\"|href=\"/cdap/$project_dir/en/|g"
  rewrite $BUILD/$HTML/404.html "action=\"search.html"  "action=\"/cdap/$project_dir/en/search.html"
}

################################################## current

function build_all() {
  echo "Building GitHub Docs."
  ./build.sh docs-github $ARG_2 $ARG_3
  echo "Stashing GitHub Docs."
  cd $SCRIPT_PATH
  mkdir -p $SCRIPT_PATH/$BUILD_TEMP
  mv $SCRIPT_PATH/$BUILD/*.zip $SCRIPT_PATH/$BUILD_TEMP
  echo "Building Web Docs."
  ./build.sh docs-web $ARG_2 $ARG_3
  echo "Replacing GitHub Docs."
  mv $SCRIPT_PATH/$BUILD_TEMP/*.zip $SCRIPT_PATH/$BUILD
  rm -rf $SCRIPT_PATH/$BUILD_TEMP
  bell
}

function build_docs_javadocs() {
  build "build"
}

function build_docs() {
  _build_docs "docs" $GOOGLE_ANALYTICS_WEB $WEB $TRUE
}

function build_docs_github() {
  _build_docs "docs-github" $GOOGLE_ANALYTICS_GITHUB $GITHUB $FALSE
}

function build_docs_web() {
  _build_docs "build-web" $GOOGLE_ANALYTICS_WEB $WEB $TRUE
}

function _build_docs() {
  build $1
  build_docs_outer_level $2
  build_zip $3
  zip_extras $4
  display_version
  bell "Building $1 completed."
}

function build() {
  build_specific_doc admin-manual $1
  build_specific_doc developers-manual $1
  build_specific_doc integrations-manual $1
  build_specific_doc reference-manual $1
  build_specific_doc examples-manual $1
}

function build_specific_doc() {
  echo ""
  echo "========================================================"
  echo "Building $1, target $2..."
  echo "========================================================"
  echo ""
  cd $SCRIPT_PATH/$1
  ./build.sh $2 $ARG_2 $ARG_3
}

function build_zip() {
  cd $SCRIPT_PATH
  set_project_path
  make_zip $1
}

function zip_extras() {
  if [ "x$1" == "x$FALSE" ]; then
    return
  fi
  # Add JSON file
  cd $SCRIPT_PATH/$BUILD/$SOURCE
  JSON_FILE=`python -c 'import conf; conf.print_json_versions_file();'`
  local json_file_path=$SCRIPT_PATH/$BUILD/$PROJECT_VERSION/$JSON_FILE
  echo `python -c 'import conf; conf.print_json_versions();'` > $json_file_path
  # Add .htaccess file (404 file)
  cd $SCRIPT_PATH
  rewrite $COMMON_SOURCE/$HTACCESS $BUILD/$PROJECT_VERSION/.$HTACCESS "<version>" "$PROJECT_VERSION"
  cd $SCRIPT_PATH/$BUILD
  zip -qr $ZIP_DIR_NAME.zip $PROJECT_VERSION/$JSON_FILE $PROJECT_VERSION/.$HTACCESS
}

function build_sdk() {
  cd developers-manual
  ./build.sh sdk $ARG_2 $ARG_3
}

function build_license_depends() {
  cd reference-manual
  ./build.sh license-pdfs $ARG_2 $ARG_3
}

function print_version() {
  cd developers-manual
  ./build.sh version $ARG_2 $ARG_3
}

function bell() {
  # Pass a message as $1
  echo -e "\a$1"
}

function test() {
  echo "Test..."
  build_json
  echo "Test completed."
}

set_project_path

run_command  $1
