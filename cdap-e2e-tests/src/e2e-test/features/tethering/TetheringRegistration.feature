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

@Tethering_Registration
Feature: Tethering registration and management

  @TETHERING_CREATION_TEST @TETHERING_CONNECTION_MANAGEMENT
  Scenario: Validate successful creation of new tethering connection requests
    Given Open tethering client Datafusion instance
    When Navigate to tethering page
    Then Open create new request page
    Then Click to select a namespace
    Then Enter project name "test-project"
    Then Enter region "us-west-1b"
    Then Enter instance name "test"
    Then Enter instance url for tethering server
    Then Enter description "test description"
    Then Finish creating new tethering request
    Then Verify the request was created successfully

  @TETHERING_CREATION_TEST
  Scenario: Validate unsuccessful creation of a new request with the same instance name
    Given Open tethering client Datafusion instance
    When Navigate to tethering page
    Then Open create new request page
    Then Click to select a namespace
    Then Enter project name "test-project"
    Then Enter region "us-west-1b"
    Then Enter instance name "test"
    Then Enter instance url for tethering server
    Then Enter description "test description"
    Then Finish creating new tethering request
    Then Verify the request failed to be created

  @TETHERING_CREATION_TEST
  Scenario: Validate unsuccessful creation of a new request with no namespaces
    Given Open tethering client Datafusion instance
    When Navigate to tethering page
    Then Open create new request page
    Then Enter project name "test-project"
    Then Enter region "us-west-1b"
    Then Enter instance name "test-no-namespace"
    Then Enter instance url for tethering server
    Then Enter description "test description"
    Then Finish creating new tethering request
    Then Verify the request failed to be created with no selected namespaces

  @TETHERING_CREATION_TEST
  Scenario: Validate unsuccessful creation of a new request with a missing required field
    Given Open tethering client Datafusion instance
    When Navigate to tethering page
    Then Open create new request page
    Then Click to select a namespace
    Then Enter region "us-west-1b"
    Then Enter instance name "test-missing-field"
    Then Enter instance url for tethering server
    Then Enter description "test description"
    Then Finish creating new tethering request
    Then Verify the request failed to be created with a missing required field

  @TETHERING_CONNECTION_MANAGEMENT
  Scenario: Validate successful deletion of a pending request
    Given Open tethering client Datafusion instance
    When Navigate to tethering page
    Then Count number of pending requests on client
    Then Click on the more menu of a pending request
    Then Click on Delete option for pending request
    Then Confirm the delete action
    Then Verify the pending request has been deleted

  @TETHERING_CONNECTION_MANAGEMENT
  Scenario: Validate successful rejection of a tethering request
    Given Connect to tethering server Datafusion instance
    Then Reject request on server from client
    Then Verify no pending tethering requests on server

  @TETHERING_CONNECTION_MANAGEMENT
  Scenario: Validate successful connection between client and server
    Given Open tethering client Datafusion instance
    When Navigate to tethering page
    Then Open create new request page
    Then Click to select a namespace
    Then Enter project name "test-project"
    Then Enter region "us-west-1b"
    Then Enter instance name "test"
    Then Enter instance url for tethering server
    Then Enter description "test description"
    Then Finish creating new tethering request
    Then Verify the request was created successfully
    When Navigate to tethering page
    Then Count number of pending requests on client
    Then Count number of established connections on server
    Given Connect to tethering server Datafusion instance
    Then Accept request on server from client
    Then Verify no pending tethering requests on server
    Given Open tethering client Datafusion instance
    When Navigate to tethering page
    Then Verify the connection is established

  @TETHERING_CONNECTION_MANAGEMENT
  Scenario: Validate successful deletion of an established connection
    Given Open tethering client Datafusion instance
    When Navigate to tethering page
    Then Count number of established connections on client
    Then Click on the more menu of a established connection
    Then Click on Delete option for established connection
    Then Confirm the delete action
    Then Verify the established connection has been deleted on client
    Given Connect to tethering server Datafusion instance
    Then Delete tethering connection on server
