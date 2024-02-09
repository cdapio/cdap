#
# Copyright © 2023 Cask Data, Inc.
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

@Sysadmin @ignore
Feature: Sysadmin - Validate system admin page design time validation scenarios

  Background:
    Given Open Datafusion Project to configure pipeline
    When Open "System Admin" menu
    Then Click on the Configuration link on the System admin page

  @SysAdminRequired
  Scenario:Validate user is able reset the system preferences added inside system admin successfully
    Then Select "systemPreferences" option from Configuration page
    Then Click on edit system preferences
    Then Set system preferences with key: "keyValue" and value: "systemPreferences1"
    Then Reset the preferences
    Then Verify the reset is successful for added preferences

  Scenario:To verify the validation error message with invalid profile name
    Then Click on the Compute Profile from the System admin page
    Then Click on create compute profile button
    Then Select a provisioner: "existingDataProc" for the compute profile
    Then Enter input plugin property: "profileLabel" with value: "invalidProfile"
    Then Enter textarea plugin property: "profileDescription" with value: "validDescription"
    Then Enter input plugin property: "clusterName" with value: "validClusterName"
    Then Click on: "Create" button in the properties
    Then Verify that the compute profile is displaying an error message: "errorInvalidProfileName" on the footer

  Scenario:To verify the validation error message with invalid namespace name
    Then Click on Create New Namespace button
    Then Enter the New Namespace Name with value: "invalidNamespaceName"
    Then Enter the Namespace Description with value: "validNamespaceDescription"
    Then Click on: "Finish" button in the properties
    Then Verify the failed error message: "errorInvalidNamespace" displayed on dialog box
