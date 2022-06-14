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

@Tethering_Manage_Requests_Server
# Ignore these tests until more auth options are enabled in order to run these steps through UI - see CDAP-19412
Feature: Tethering - Manage Tethering Requests on Server

  @TETHERING_MANAGE_REQUESTS_SERVER_TEST
  Scenario: Validate successful acceptance of a tethering request
    Given Open Datafusion Project to configure pipeline
    When Navigate to tethering page
    Then Count number of pending requests on server
    Then Count number of established connections on server
    Then Click on accept button
    Then Verify the request has been accepted

  @TETHERING_MANAGE_REQUESTS_SERVER_TEST
  Scenario: Validate successful rejection of a tethering request
    Given Open Datafusion Project to configure pipeline
    When Navigate to tethering page
    Then Count number of pending requests on server
    Then Count number of established connections on server
    Then Click on reject button
    Then Confirm the reject action
    Then Verify the request has been rejected

  @TETHERING_MANAGE_REQUESTS_SERVER_TEST
  Scenario: Validate successful deletion of an established connection
    Given Open Datafusion Project to configure pipeline
    When Navigate to tethering page
    Then Count number of established connections on server
    Then Click on the more menu of a established connection
    Then Click on Delete option for established connection
    Then Confirm the delete action
    Then Verify the established connection has been deleted on server
