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
COMMON_SOURCE="$COMMON/_source"
COMMON_CONF_PY="$COMMON/common_conf.py"
COMMON_HIGHLEVEL_PY="$COMMON/highlevel_conf.py"
COMMON_PLACEHOLDER="$COMMON/_source/placeholder_index.rst"

# REDIRECT_DEVELOPER_HTML=`cat <<EOF
# <!DOCTYPE HTML>
# <html lang="en-US">
#     <head>
#         <meta charset="UTF-8">
#         <meta http-equiv="refresh" content="0;url=developers-manual/index.html">
#         <script type="text/javascript">
#             window.location.href = "developers-manual/index.html"
#         </script>
#         <title></title>
#     </head>
#     <body>
#     </body>
# </html>
# EOF`

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
  echo "    docs           Clean build of just the HTML docs, skipping Javadocs"
  echo "    docs-github    Clean build of HTML docs and Javadocs, zipped for placing on GitHub"
  echo "    docs-web       Clean build of HTML docs and Javadocs, zipped for placing on docs.cask.co webserver"
  echo ""
  echo "    zip            Zips results; options: none, $WEB, or $GITHUB"
  echo "    licenses       Clean build of License Dependency PDFs"
  echo ""
  echo "    sdk            Build SDK"
  echo "    version        Print the version information"
  echo ""
  echo "  with"
  echo "    source         Path to $PROJECT source, if not $PROJECT_PATH"
  echo "    test_includes  local or remote (default: remote); must specify source if used"
  echo ""
}

function run_command() {
  case "$1" in
    all )               build_all; exit 1;;
    docs )              build_docs; exit 1;;
    docs-github )       build_docs_github; exit 1;;
    docs-web )          build_docs_web; exit 1;;
    zip )               build_zip $2; exit 1;;
    licenses )          build_license_depends; exit 1;;
    sdk )               build_sdk; exit 1;;
    version )           print_version; exit 1;;
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
  clean
  copy_source admin-manual      "Administration Manual"
  copy_source developers-manual "Developers’ Manual"
  copy_source reference-manual  "Reference Manual"
  copy_source examples-manual   "Examples, Guides, and Tutorials"
  # build all docs
  cd $SCRIPT_PATH
  cp $COMMON_HIGHLEVEL_PY $BUILD/$SOURCE/conf.py
  cp $COMMON_SOURCE/index.rst $BUILD/$SOURCE/
  cp $COMMON_SOURCE/table-of-contents.rst $BUILD/$SOURCE/
  sphinx-build -b html -d build/doctrees build/source build/html
  copy_html admin-manual
  copy_html developers-manual
  copy_html reference-manual
  copy_html examples-manual
}


################################################## current

function clean_old() {
  cd $SCRIPT_PATH
  rm -rf $SCRIPT_PATH/$BUILD
  mkdir -p $SCRIPT_PATH/$BUILD/$HTML
  echo "Cleaned $BUILD directory"
  echo ""
}

function build_all() {
  echo "Building GitHub Docs."
  ./build.sh docs-github $ARG_2 $ARG_3
  echo "Stashing GitHub Docs."
  cd $SCRIPT_PATH
  mkdir -p $SCRIPT_PATH/$BUILD_TEMP
  mv $SCRIPT_PATH/$BUILD/*.zip $SCRIPT_PATH/$BUILD_TEMP
  echo "Building Web Docs."
  ./build.sh docs-web $ARG_2 $ARG_3
  echo "Moving GitHub Docs."
  mv $SCRIPT_PATH/$BUILD_TEMP/*.zip $SCRIPT_PATH/$BUILD
  rm -rf $SCRIPT_PATH/$BUILD_TEMP
}

function build_docs() {
#   clean_old
  build "docs"
  build_docs_outer_level
  build_zip $WEB
}

function build_docs_javadocs() {
#   clean_old
  build "build"
}

function build_docs_github() {
  build "build-github"
  build_docs_outer_level
  build_zip $GITHUB
}

function build_docs_web() {
  build "build-web"
  build_docs_outer_level
  build_zip $WEB
}

function build() {
  build_specific_doc admin-manual $1
  build_specific_doc developers-manual $1
  build_specific_doc reference-manual $1
  build_specific_doc examples-manual $1
#   add_redirect
}

# function add_redirect() {
#   cd $SCRIPT_PATH/$BUILD/$HTML
#   echo "$REDIRECT_DEVELOPER_HTML" > index.html
# }

function build_specific_doc() {
  echo "Building $1, target $2..."
  cd $SCRIPT_PATH/$1
  ./build.sh $2 $ARG_2 $ARG_3
#   cd $SCRIPT_PATH
#   echo "Copying $1 results..."
#   cp -r $1/$BUILD/$HTML $BUILD/$HTML/$1
#   echo ""
}

function build_zip() {
  cd $SCRIPT_PATH
  # re-sourcing replaces the commands in this file with common ones
  source _common/common-build.sh
  set_project_path
  print_version
  if [ "x$1" == "x" ]; then
    echo "make_zip"
    make_zip
  else
    echo "make_zip_localized $1"
    make_zip_localized $1
  fi
  echo "Building zip completed."
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
  echo "print_version '$ARG_1' '$ARG_2' '$ARG_3'"
  cd developers-manual
  ./build.sh $ARG_1 $ARG_2 $ARG_3
}

set_project_path

run_command  $1
