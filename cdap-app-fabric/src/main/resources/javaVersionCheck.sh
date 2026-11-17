# Copyright © 2026 Cask Data, Inc.
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
# Inlined into the remote launch script by RemoteExecutionTwillPreparer.
# Picks the java command (JAVA_HOME first, then PATH) and exits unless it is
# Java 11 or later. Lines such as "Picked up JAVA_TOOL_OPTIONS: ..." can come
# before the version line and are skipped.
if [ -n "${JAVA_HOME}" ] && [ -x "${JAVA_HOME}/bin/java" ]; then
  JAVA_CMD="${JAVA_HOME}/bin/java"
else
  JAVA_CMD="java"
fi
JAVA_VER_RAW=$("${JAVA_CMD}" -version 2>&1 | awk -F '"' '!/^Picked up / && /version "/ {print $2; exit}')
JAVA_MAJOR=$(echo "${JAVA_VER_RAW}" | awk -F '[._+-]' '{if ($1 == "1") print $2; else print $1}')
case "${JAVA_MAJOR}" in ''|*[!0-9]*) JAVA_MAJOR=0 ;; esac
if [ "${JAVA_MAJOR}" -lt 11 ]; then
  echo "Java 11+ required on the cluster (found: ${JAVA_VER_RAW:-unknown})" >&2
  exit 1
fi
