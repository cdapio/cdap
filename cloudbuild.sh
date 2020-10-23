#!/bin/bash
#
# Copyright © 2020 Cask Data, Inc.
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

useradd -ms /bin/bash builder

chown -R builder /home/builder
chown -R builder /workspace
chown -R builder ${MAVEN_CACHE_DIR}

su builder -c 'mvn clean test -fae -B -V -P templates,skip-hbase-compat-tests'
