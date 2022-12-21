#
# Copyright © 2022 Cask Data, Inc.
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

@Tethering_Runtime
# TODO - Add pipeline deployment tests once additional auth options are enabled in e2e framework - see CDAP-19412
Feature: Tethering profile and runtime

  @TETHERING_PROFILE_TEST
  Scenario: Validate creation of tethering compute profile
    Given Connect to tethering server Datafusion instance
    Then Create compute profile to tethered client
    Then Verify compute profile was created successfully
    Then Delete compute profile
