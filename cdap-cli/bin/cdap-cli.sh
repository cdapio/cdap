#!/usr/bin/env bash

#
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
#

#!/usr/bin/env bash

#
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
#

# Attempt to set APP_HOME
# Resolve links: $0 may be a link
PRG="$0"
bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`
lib="$bin"/../lib
conf="$bin"/../conf
CDAP_CONF=${CDAP_CONF:-/etc/cdap/conf}
script=`basename $0`

# Need this for relative symlinks.
while [ -h "$PRG" ] ; do
    ls=`ls -ld "$PRG"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '/.*' > /dev/null; then
        PRG="$link"
    else
        PRG=`dirname "$PRG"`"/$link"
    fi
done

CLASSPATH_COMMON=${lib}/aopalliance.aopalliance-1.0.jar:${lib}/co.cask.cdap.cdap-api-2.5.1.jar:${lib}/co.cask.cdap.cdap-authentication-client-1.0.1.jar:${lib}/co.cask.cdap.cdap-cli-2.5.1.jar:${lib}/co.cask.cdap.cdap-client-2.5.1.jar:${lib}/co.cask.cdap.cdap-common-2.5.1.jar:${lib}/co.cask.cdap.cdap-proto-2.5.1.jar:${lib}/co.cask.tephra.tephra-api-0.3.0.jar:${lib}/com.google.code.findbugs.jsr305-2.0.1.jar:${lib}/com.google.code.gson.gson-2.2.4.jar:${lib}/com.google.guava.guava-13.0.1.jar:${lib}/com.google.inject.extensions.guice-assistedinject-3.0.jar:${lib}/com.google.inject.extensions.guice-multibindings-3.0.jar:${lib}/com.google.inject.guice-3.0.jar:${lib}/com.googlecode.concurrent-trees.concurrent-trees-2.4.0.jar:${lib}/commons-codec.commons-codec-1.6.jar:${lib}/commons-logging.commons-logging-1.1.1.jar:${lib}/javax.inject.javax.inject-1.jar:${lib}/javax.ws.rs.javax.ws.rs-api-2.0.jar:${lib}/jline.jline-2.12.jar:${lib}/org.apache.httpcomponents.httpclient-4.2.5.jar:${lib}/org.apache.httpcomponents.httpcore-4.2.5.jar:${lib}/org.slf4j.slf4j-api-1.7.5.jar:${lib}/org.slf4j.slf4j-nop-1.7.5.jar

if [ "$CLASSPATH" = "" ]; then
  CLASSPATH=$CLASSPATH_COMMON
else
  CLASSPATH=$CLASSPATH:$CLASSPATH_COMMON
fi

# Load the configuration too.
if [ -d "$CDAP_CONF" ]; then
  CLASSPATH=$CLASSPATH:"$CDAP_CONF"
elif [ -d "$conf" ]; then
  CLASSPATH=$CLASSPATH:"$conf"/
fi

java -cp ${CLASSPATH} -Dscript=$script co.cask.cdap.shell.CLIMain "$@"
