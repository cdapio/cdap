# Copyright © 2018 Cask Data, Inc.
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

# By default, kill the CDAP Sandbox process if it runs out of memory
export KILL_ON_OOM_OPTS="-XX:OnOutOfMemoryError=\"kill -9 %p\""

# Uncomment to perform Java heap dump on OutOfMemory errors
# export HEAPDUMP_ON_OOM=true
