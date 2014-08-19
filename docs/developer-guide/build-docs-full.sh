#!/usr/bin/env bash

# Build script for docs
# Builds the docs
# Copies the javadocs into place
# Zips everything up so it can be staged

DATE_STAMP=`date`
SCRIPT=`basename $0`

SOURCE="source"
BUILD="build"
BUILD_PDF="build-pdf"
HTML="html"
APIDOCS="apidocs"
JAVADOCS="javadocs"
LICENSES="licenses"
LICENSES_PDF="licenses-pdf"
PRODUCT="cdap"
PRODUCT_CAPS="CDAP"

SCRIPT_PATH=`pwd`
SOURCE_PATH="$SCRIPT_PATH/$SOURCE"
BUILD_PATH="$SCRIPT_PATH/$BUILD"
HTML_PATH="$BUILD_PATH/$HTML"

DOCS_PY="$SCRIPT_PATH/../tools/scripts/docs.py"

DOCS_INDEX_PAGE="index.html"
DOCS_INDEX="$SCRIPT_PATH/../tools/staging/$DOCS_INDEX_PAGE"

REST_SOURCE="$SOURCE_PATH/rest.rst"
REST_PDF="$SCRIPT_PATH/$BUILD_PDF/rest.pdf"

WWW_PATH="/var/www/website-docs"

if [ "x$2" == "x" ]; then
  PRODUCT_PATH="$SCRIPT_PATH/../../"
else
  PRODUCT_PATH="$2"
fi
PRODUCT_JAVADOCS="$PRODUCT_PATH/target/site/apidocs"

ZIP_FILE_NAME=$HTML
ZIP="$ZIP_FILE_NAME.zip"
STAGING_SERVER="stg-web101.sw.joyent.continuuity.net"

function usage() {
  cd $PRODUCT_PATH
  PRODUCT_PATH=`pwd`
  echo "Build script for '$PRODUCT_CAPS' docs"
  echo "Usage: $SCRIPT < option > [source]"
  echo ""
  echo "  Options (select one)"
  echo "    build        Clean build of javadocs, docs (HTML and PDF), copy javadocs and pdfs, zip results"
  echo "    stage        Stages docs and logins to server"
  echo "  or "
  echo "    build_docs   Clean build of docs"
  echo "    javadocs     Clean build of javadocs"
  echo "    pdf_licenses Clean build of Licence Dependency PDFs"
  echo "    pdf_rest     Clean build of REST PDF"
  echo "    pdf_install  Clean build of Install Guide PDF"
  echo "    login        Logs you into $STAGING_SERVER"
  echo "    zip          Zips docs into $ZIP"
  echo "  or"
  echo "    depends      Build Site listing dependencies"
  echo "    sdk          Build SDK"
  echo "  with"
  echo "    source       Path to $PRODUCT source for javadocs, if not $PRODUCT_PATH"
  echo " "
  exit 1
}

function clean() {
  rm -rf $SCRIPT_PATH/$BUILD
}

function build_javadocs() {
  cd $PRODUCT_PATH
  mvn clean site -DskipTests
}

function build_docs() {
  clean
  sphinx-build -b html -d build/doctrees source build/html
}

function build_pdf_rest() {
  # renaming is done by the pom.xml file
  rm -rf $SCRIPT_PATH/$BUILD_PDF
  mkdir $SCRIPT_PATH/$BUILD_PDF
  python $DOCS_PY -g pdf -o $REST_PDF $REST_SOURCE
}

function build_pdf_install() {
  INSTALL_GUIDE="$SCRIPT_PATH/../install-guide"
  INSTALL_SOURCE="$INSTALL_GUIDE/source/install.rst"
  version
  INSTALL_PDF="$INSTALL_GUIDE/$BUILD_PDF/$PRODUCT_CAPS-Installation-Guide-v$PRODUCT_VERSION.pdf"
  rm -rf $INSTALL_GUIDE/$BUILD_PDF
  mkdir $INSTALL_GUIDE/$BUILD_PDF
  python $DOCS_PY -g pdf -o $INSTALL_PDF $INSTALL_SOURCE
}

function build_pdf_licenses() {
  rm -rf $SCRIPT_PATH/$LICENSES_PDF
  mkdir $SCRIPT_PATH/$LICENSES_PDF
  
  LIC1_SOURCE="$SOURCE_PATH/licenses/$PRODUCT-enterprise-dependencies.rst"
  LIC1_PDF="$SCRIPT_PATH/$LICENSES_PDF/$PRODUCT-enterprise-dependencies.pdf"
  python $DOCS_PY -g pdf -o $LIC1_PDF $LIC1_SOURCE

  LIC2_SOURCE="$SOURCE_PATH/licenses/$PRODUCT-level-1-dependencies.rst"
  LIC2_PDF="$SCRIPT_PATH/$LICENSES_PDF/$PRODUCT-level-1-dependencies.pdf"
  python $DOCS_PY -g pdf -o $LIC2_PDF $LIC2_SOURCE
  
  LIC3_SOURCE="$SOURCE_PATH/licenses/$PRODUCT-singlenode-dependencies.rst"
  LIC3_PDF="$SCRIPT_PATH/$LICENSES_PDF/$PRODUCT-singlenode-dependencies.pdf"
  python $DOCS_PY -g pdf -o $LIC3_PDF $LIC3_SOURCE
}

function copy_javadocs() {
  cd $BUILD_PATH/$HTML
  rm -rf $JAVADOCS
  cp -r $PRODUCT_JAVADOCS .
  mv -f $APIDOCS $JAVADOCS
}

function copy_license_pdfs() {
  cd $BUILD_PATH/$HTML/$LICENSES
  cp $SCRIPT_PATH/$LICENSES_PDF/* .
}

function make_zip() {
  cd $SCRIPT_PATH/$BUILD
  zip -r $ZIP_FILE_NAME $HTML/*
}

function stage_docs() {
  echo "Deploying docs..."
  echo "rsync -vz $SCRIPT_PATH/$BUILD/$ZIP \"$USER@$STAGING_SERVER:$ZIP\""
  rsync -vz $SCRIPT_PATH/$BUILD/$ZIP "$USER@$STAGING_SERVER:$ZIP"
  version
  cd_cmd="cd $WWW_PATH/$PRODUCT; ls"
  remove_cmd="sudo rm -rf $PRODUCT_VERSION"
  unzip_cmd="sudo unzip ~/$ZIP; sudo mv $HTML $PRODUCT_VERSION"
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
  echo "or, using current branch:"
  echo ""
  echo "  $cd_cmd"
  echo "  $remove_cmd-$GIT_BRANCH; ls"
  echo "  $unzip_cmd-$GIT_BRANCH; ls"
  echo ""
  login_staging_server
}

function login_staging_server() {
  echo "Logging into:"
  echo "ssh \"$USER@$STAGING_SERVER\""
  ssh "$USER@$STAGING_SERVER"
}

function build() {
   build_docs
   build_javadocs
   copy_javadocs
   copy_license_pdfs
   make_zip
}

function build_single_node() {
  cd $PRODUCT_PATH
  mvn clean package -DskipTests -P examples && mvn package -pl singlenode -am -DskipTests -P dist,release
}

function build_sdk() {
  build_single_node
}

function build_dependencies() {
  cd $PRODUCT_PATH
  mvn clean package site -am -Pjavadocs -DskipTests
}

function version() {
  cd $PRODUCT_PATH
  PRODUCT_VERSION=`mvn help:evaluate -o -Dexpression=project.version | grep -v '^\['`
  IFS=/ read -a branch <<< "`git rev-parse --abbrev-ref HEAD`"
  GIT_BRANCH="${branch[1]}"
}

function print_version() {
  version
  echo "PRODUCT_VERSION: $PRODUCT_VERSION"
}

if [ $# -lt 1 ]; then
  usage
  exit 1
fi

case "$1" in
  build )              build; exit 1;;
  build_javadocs )     build_javadocs; exit 1;;
  build_docs )         build_docs; exit 1;;
  build_single_node )  build_single_node; exit 1;;
  build_license_pdfs ) build_license_pdfs; exit 1;;
  copy_javadocs )      copy_javadocs; exit 1;;
  copy_license_pdfs )  copy_license_pdfs; exit 1;;
  javadocs )           build_javadocs; exit 1;;
  depends )            build_dependencies; exit 1;;
  login )              login_staging_server; exit 1;;
  pdf_install )        build_pdf_install; exit 1;;
  pdf_licenses )       build_pdf_licenses; exit 1;;
  pdf_rest )           build_pdf_rest; exit 1;;
  sdk )                build_sdk; exit 1;;
  stage )              stage_docs; exit 1;;
  version )            print_version; exit 1;;
  zip )                make_zip; exit 1;;
  * )                  usage; exit 1;;
esac
