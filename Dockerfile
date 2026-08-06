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

FROM us-east1-docker.pkg.dev/j145774183a931adb-tp/cdf-dev-shru/cloud-data-fusion:latest
# For OSS CDAP, use "FROM gcr.io/cdapio/cdap:latest"

RUN rm -rf /opt/cdap/master/lib/io.cdap.cdap.cdap-common-6.12.0-SNAPSHOT.jar

COPY cdap-common/target/cdap-common-6.12.0-SNAPSHOT.jar /opt/cdap/master/lib/io.cdap.cdap.cdap-common-6.12.0-SNAPSHOT.jar

RUN chmod -R 755 /opt/cdap
