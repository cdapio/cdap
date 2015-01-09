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

API="cdap-api"
APIDOCS="apidocs"
APIS="apis"
BUILD="build"
BUILD_PDF="build-pdf"
HTML="html"
INCLUDES="_includes"
JAVADOCS="javadocs"
LICENSES="licenses"
LICENSES_PDF="licenses-pdf"
PROJECT="cdap"
PROJECT_CAPS="CDAP"
REFERENCE="reference-manual"
SOURCE="source"

FALSE="false"
TRUE="true"

# Redirect placed in top to redirect to 'en' directory
REDIRECT_EN_HTML=`cat <<EOF
<!DOCTYPE HTML>
<html lang="en-US">
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="refresh" content="0;url=en/index.html">
        <script type="text/javascript">
            window.location.href = "en/index.html"
        </script>
        <title></title>
    </head>
    <body>
    </body>
</html>
EOF`

SCRIPT=`basename $0`
SCRIPT_PATH=`pwd`

DOC_GEN_PY="$SCRIPT_PATH/../tools/doc-gen.py"
BUILD_PATH="$SCRIPT_PATH/$BUILD"
HTML_PATH="$BUILD_PATH/$HTML"
SOURCE_PATH="$SCRIPT_PATH/$SOURCE"

if [ "x$2" == "x" ]; then
  PROJECT_PATH="$SCRIPT_PATH/../../"
else
  PROJECT_PATH="$SCRIPT_PATH/../../../$2"
fi

SDK_JAVADOCS="$PROJECT_PATH/$API/target/site/$APIDOCS"

CHECK_INCLUDES="false"
TEST_INCLUDES_LOCAL="local"
TEST_INCLUDES_REMOTE="remote"
if [ "x$3" == "x" ]; then
  TEST_INCLUDES="$TEST_INCLUDES_REMOTE"
else
  TEST_INCLUDES="$3"
fi

RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m'
WARNING="${RED}${BOLD}WARNING:${NC}"

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
  echo "Usage: $SCRIPT < option > [source test_includes]"
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
  echo "    license-pdfs   Clean build of License Dependency PDFs"
  echo "    zip            Zips docs into $ZIP"
  echo ""
  echo "    check-includes Check if included files have changed from source"
  echo "    depends        Build Site listing dependencies"
  echo "    sdk            Build SDK"
  echo "  with"
  echo "    source         Path to $PROJECT source for javadocs, if not $PROJECT_PATH"
  echo "    test_includes  local, remote or neither (default: remote); must specify source if used"
  echo " "
#   exit 1
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
  MAVEN_OPTS="-Xmx512m" mvn clean site -DskipTests
}

function build_javadocs_sdk() {
  cd $PROJECT_PATH
  MAVEN_OPTS="-Xmx512m"  mvn clean package javadoc:javadoc -pl $API -am -DskipTests -P release
  copy_javadocs_sdk
}

function copy_javadocs_sdk() {
  cd $BUILD_PATH/$HTML
  rm -rf $JAVADOCS
  cp -r $SDK_JAVADOCS .
  mv -f $APIDOCS $JAVADOCS
}

function build_license_pdfs() {
  version
  cd $SCRIPT_PATH
  PROJECT_VERSION_TRIMMED=${PROJECT_VERSION%%-SNAPSHOT*}
  rm -rf $SCRIPT_PATH/$LICENSES_PDF
  mkdir $SCRIPT_PATH/$LICENSES_PDF
  E_DEP="cdap-enterprise-dependencies"
  L_DEP="cdap-level-1-dependencies"
  S_DEP="cdap-standalone-dependencies"
  # paths are relative to location of $DOC_GEN_PY script
  LIC_PDF="../../../$REFERENCE/$LICENSES_PDF"
  LIC_RST="../$REFERENCE/source/$LICENSES"
  echo ""
  echo "Building $E_DEP"
  python $DOC_GEN_PY -g pdf -o $LIC_PDF/$E_DEP.pdf -b $PROJECT_VERSION_TRIMMED $LIC_RST/$E_DEP.rst
  echo ""
  echo "Building $L_DEP"
  python $DOC_GEN_PY -g pdf -o $LIC_PDF/$L_DEP.pdf -b $PROJECT_VERSION_TRIMMED $LIC_RST/$L_DEP.rst
  echo ""
  echo "Building $S_DEP"
  python $DOC_GEN_PY -g pdf -o $LIC_PDF/$S_DEP.pdf -b $PROJECT_VERSION_TRIMMED $LIC_RST/$S_DEP.rst
}

function copy_license_pdfs() {
  cd $BUILD_PATH/$HTML/$LICENSES
  cp $SCRIPT_PATH/$LICENSES_PDF/* .
}

function make_zip_html() {
  version
  ZIP_FILE_NAME="$PROJECT-docs-$PROJECT_VERSION.zip"
  cd $SCRIPT_PATH/$BUILD
  zip -qr $ZIP_FILE_NAME $HTML/*
}

function make_zip() {
# This creates a zip that unpacks to the same name
  version
  if [ "x$1" == "x" ]; then
    ZIP_DIR_NAME="$PROJECT-docs-$PROJECT_VERSION"
  else
    ZIP_DIR_NAME="$PROJECT-docs-$PROJECT_VERSION-$1"
  fi  
  cd $SCRIPT_PATH/$BUILD
  mkdir $ZIP_DIR_NAME
  mv $HTML $ZIP_DIR_NAME/en
  # Add a redirect index.html file
  echo "$REDIRECT_EN_HTML" > $ZIP_DIR_NAME/index.html
  zip -qr $ZIP_DIR_NAME.zip $ZIP_DIR_NAME/*
}

function make_zip_localized() {
  _make_zip_localized $1
  zip -qr $ZIP_DIR_NAME.zip $PROJECT_VERSION/*
}

function make_zip_localized_web() {
  _make_zip_localized $1
  # Add JSON file
  build_json $SCRIPT_PATH/$BUILD/$PROJECT_VERSION
  cd $SCRIPT_PATH/$BUILD
  zip -qr $ZIP_DIR_NAME.zip $PROJECT_VERSION/*
}

function _make_zip_localized() {
  version
  ZIP_DIR_NAME="$PROJECT-docs-$PROJECT_VERSION-$1"
  cd $SCRIPT_PATH/$BUILD
  mkdir $PROJECT_VERSION
  mv $HTML $PROJECT_VERSION/en
  # Add a redirect index.html file
  echo "$REDIRECT_EN_HTML" > $PROJECT_VERSION/index.html
}

function build_json() {
  cd $SCRIPT_PATH/$BUILD/$SOURCE
  JSON_FILE=`python -c 'import conf; conf.print_json_versions_file();'`
  echo `python -c 'import conf; conf.print_json_versions();'` > $1/$JSON_FILE
}

function build_extras() {
  # Over-ride this function in guides where Javadocs or licenses are being built or copied.
  # Currently performed in reference-manual
  echo "No extras being built."
}

function build() {
  build_docs
  build_extras
}

function build_github() {
  build_docs_google $GOOGLE_ANALYTICS_GITHUB
  build_extras
}

function build_web() {
  build_docs_google $GOOGLE_ANALYTICS_WEB
  build_extras
}

function check_includes() {
  if [ $CHECK_INCLUDES == $TRUE ]; then
    if hash pandoc 2>/dev/null; then
      echo "Confirmed that pandoc is installed; checking includes."
      # Build includes
      BUILD_INCLUDES_DIR=$SCRIPT_PATH/$BUILD/$INCLUDES
      rm -rf $BUILD_INCLUDES_DIR
      mkdir $BUILD_INCLUDES_DIR
      pandoc_includes $BUILD_INCLUDES_DIR
      # Test included files
      test_includes
    else
      echo -e "$WARNING pandoc is not installed; checked-in includes will be used instead."
    fi
  else
    echo "No includes to be checked."
  fi
}

function test_includes () {
  # For an example of over-riding this function, see developer/build.sh
  echo "No includes to be tested."
}

function test_an_include() {
  BUILD_INCLUDES_DIR=$SCRIPT_PATH/$BUILD/$INCLUDES
  SOURCE_INCLUDES_DIR=$SCRIPT_PATH/$SOURCE/$INCLUDES
  EXAMPLE=$1
  if [ "x$TEST_INCLUDES" == "x$TEST_INCLUDES_LOCAL" -o "x$TEST_INCLUDES" == "x$TEST_INCLUDES_REMOTE" ]; then
    if diff -q $BUILD_INCLUDES_DIR/$1 $SOURCE_INCLUDES_DIR/$1 2>/dev/null; then
      echo "Tested $1; matches checked-in include file."
    else
      echo -e "$WARNING Tested $1; does not match checked-in include file. Copying to source directory."
      cp -f $BUILD_INCLUDES_DIR/$1 $SOURCE_INCLUDES_DIR/$1
    fi
  else
    echo -e "$WARNING Not testing includes: using checked-in version..."
    cp -f $SOURCE_INCLUDES_DIR/$1 $BUILD_INCLUDES_DIR/$1
  fi
}

function build_includes() {
  if hash pandoc 2>/dev/null; then
    echo "Confirmed that pandoc is installed; rebuilding the README includes."
    SOURCE_INCLUDES_DIR=$SCRIPT_PATH/$SOURCE/$INCLUDES
    rm -rf $SOURCE_INCLUDES_DIR
    mkdir $SOURCE_INCLUDES_DIR
    pandoc_includes $SOURCE_INCLUDES_DIR
  else
    echo -e "$WARNING pandoc not installed; checked-in README includes will be used instead."
  fi
}

function pandoc_includes() {
  # $1 passed is the directory to which the translated files are to be written.
  # For an example of over-riding this function, see developer/build.sh
  echo "No includes to be translated."
}

function build_standalone() {
  cd $PROJECT_PATH
  MAVEN_OPTS="-Xmx512m" mvn clean package -DskipTests -P examples -pl cdap-examples -am -amd && mvn package -pl cdap-standalone -am -DskipTests -P dist,release
}

function build_sdk() {
  build_standalone
}

function build_dependencies() {
  cd $PROJECT_PATH
  mvn clean package site -am -Pjavadocs -DskipTests
}

function version() {
  cd $PROJECT_PATH
  PROJECT_VERSION=`grep "<version>" pom.xml`
  PROJECT_VERSION=${PROJECT_VERSION#*<version>}
  PROJECT_VERSION=${PROJECT_VERSION%%</version>*}
  IFS=/ read -a branch <<< "`git rev-parse --abbrev-ref HEAD`"
  GIT_BRANCH="${branch[1]}"
}

function display_version() {
  version
  echo "PROJECT_PATH: $PROJECT_PATH"
  echo "PROJECT_VERSION: $PROJECT_VERSION"
  echo "GIT_BRANCH: $GIT_BRANCH"
}

function rewrite() {
  # Substitutes text in file $1 and outputting to file $2, replacing text $3 with text $4.
  cd $SCRIPT_PATH
  local rewrite_source=$1
  local rewrite_target=$2
  local sub_string=$3
  local new_sub_string=$4  
  echo "Re-writing"
  echo "    $rewrite_source"
  echo "  to"
  echo "    $rewrite_target"
  echo "  $sub_string -> $new_sub_string "
  sed -e "s|$sub_string|$new_sub_string|g" $rewrite_source > $rewrite_target
}

function run_command() {
  case "$1" in
    build )             build; exit 1;;
    build-includes )    build_includes; exit 1;;
    build-github )      build_github; exit 1;;
    build-web )         build_web; exit 1;;
    check-includes )    check_includes; exit 1;;
    docs )              build_docs;;
    license-pdfs )      build_license_pdfs; exit 1;;
    build-standalone )  build_standalone; exit 1;;
    copy-javadocs )     copy_javadocs; exit 1;;
    copy-license-pdfs ) copy_license_pdfs; exit 1;;
    javadocs )          build_javadocs_sdk; exit 1;;
    javadocs-full )     build_javadocs_full; exit 1;;
    depends )           build_dependencies; exit 1;;
    sdk )               build_sdk; exit 1;;
    version )           display_version; exit 1;;
    zip )               make_zip; exit 1;;
    * )                 usage; exit 1;;
  esac
}