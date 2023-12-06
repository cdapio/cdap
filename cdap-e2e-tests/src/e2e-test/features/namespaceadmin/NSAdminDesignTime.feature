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
@Namespaceadmin
Feature: NameSpaceAdmin - Validate nameSpace admin design time scenarios

  @Namespaceadmin
  Scenario:Verify user is able to click on the namespace admin tab and successfully navigates to the page
    Given Open Datafusion Project to configure pipeline
    When Click on the Hamburger bar on the left panel
    Then Click on NameSpace Admin link from the menu
    Then Verify if user successfully navigated to namespace admin page

  @Namespaceadmin
  Scenario:Validate user is able to open compute profile page and create a profile for selected a provisioner
    Given Open Datafusion Project to configure pipeline
    Then Click on the Hamburger bar on the left panel
    Then Click on NameSpace Admin link from the menu
    Then Click on create profile button for "default" Namespace
    Then Select a provisioner: "remoteHadoopProvisioner" for the compute profile
    Then Verify the Create a Profile page is loaded for selected provisioner
    Then Enter input plugin property: "profileLabel" with value: "validProfile"
    Then Enter textarea plugin property: "profileDescription" with value: "validDescription"
    Then Enter input plugin property: "host" with value: "testHost"
    Then Enter input plugin property: "user" with value: "testUser"
    Then Enter textarea plugin property: "sshKey" with value: "testSSHKey"
    Then Click on: "Create" button in the properties
    Then Verify the created compute profile: "validProfile" is displayed in system compute profile list


  @Namespaceadmin
  Scenario: Validate user is able to create new namespace preferences and able to delete the added namespace preferences successfully
    Given Open Datafusion Project to configure pipeline
    Then Click on the Hamburger bar on the left panel
    Then Click on NameSpace Admin link from the menu
    Then Click "preferences" tab from Configuration page for "default" Namespace
    Then Click on edit namespace preferences to set namespace preferences
    Then Set namespace preferences with key: "keyValue" and value: "nameSpacePreferences1"
    Then Click on the Save & Close preferences button
    Then Click on edit namespace preferences to set namespace preferences
    Then Delete the preferences
    Then Click on the Save & Close preferences button

  Scenario: Validate user is able to add multiple namespace preferences inside namespace admin successfully
    Given Open Datafusion Project to configure pipeline
    Then Click on the Hamburger bar on the left panel
    Then Click on NameSpace Admin link from the menu
    Then Click "preferences" tab from Configuration page for "default" Namespace
    Then Click on edit namespace preferences to set namespace preferences
    Then Set namespace preferences with key: "keyValue" and value: "nameSpacePreferences2"
    Then Click on the Save & Close preferences button
    Then Click on edit namespace preferences to set namespace preferences
    Then Delete the preferences
    Then Delete the preferences
    Then Click on the Save & Close preferences button

  Scenario: Validate user is able reset the namespace preferences added inside namespace admin successfully
    Given Open Datafusion Project to configure pipeline
    Then Click on the Hamburger bar on the left panel
    Then Click on NameSpace Admin link from the menu
    Then Click "preferences" tab from Configuration page for "default" Namespace
    Then Click on edit namespace preferences to set namespace preferences
    Then Set namespace preferences with key: "keyValue" and value: "nameSpacePreferences1"
    Then Reset the preferences
    Then Verify the reset is successful for added preferences

  Scenario: To verify the validation error message with invalid cluster name
    Given Open Datafusion Project to configure pipeline
    Then Click on the Hamburger bar on the left panel
    Then Click on NameSpace Admin link from the menu
    Then Click on create profile button for "default" Namespace
    Then Select a provisioner: "existingDataProc" for the compute profile
    Then Enter input plugin property: "profileLabel" with value: "validProfile"
    Then Enter textarea plugin property: "profileDescription" with value: "validDescription"
    Then Enter input plugin property: "clusterName" with value: "invalidClusterName"
    Then Click on: "Create" button in the properties
    Then Verify that the compute profile is displaying an error message: "errorInvalidClusterName" on the footer

  Scenario:To verify the validation error message with invalid profile name
    Given Open Datafusion Project to configure pipeline
    Then Click on the Hamburger bar on the left panel
    Then Click on NameSpace Admin link from the menu
    Then Click on create profile button for "default" Namespace
    Then Select a provisioner: "existingDataProc" for the compute profile
    Then Enter input plugin property: "profileLabel" with value: "invalidProfile"
    Then Enter textarea plugin property: "profileDescription" with value: "validDescription"
    Then Enter input plugin property: "clusterName" with value: "validClusterName"
    Then Click on: "Create" button in the properties
    Then Verify that the compute profile is displaying an error message: "errorInvalidProfileName" on the footer

  Scenario:To verify the validation error message with invalid namespace name
    Given Open Datafusion Project to configure pipeline
    Then Click on the Hamburger bar on the left panel
    Then Click on Namespace dropdown button
    Then Click on the Add Namespace tab
    Then Enter the New Namespace Name with value: "invalidNamespaceName"
    Then Enter the Namespace Description with value: "validNamespaceDescription"
    Then Click on: "Finish" button in the properties
    Then Verify the failed error message: "errorInvalidNamespace" displayed on dialog box

  Scenario: Validate user is able to create new namespace from hamburger menu and switch to newly created namespace
    Given Open Datafusion Project to configure pipeline
    Then Click on the Hamburger bar on the left panel
    Then Click on Namespace dropdown button
    Then Click on the Add Namespace tab
    Then Enter the New Namespace Name with value: "validNamespaceName"
    Then Enter the Namespace Description with value: "validNamespaceDescription"
    Then Click on: "Finish" button in the properties
    Then Switch to the newly created Namespace
    Then Click on the Hamburger bar on the left panel
    Then Verify the namespace is switched to "validNamespaceName" successfully
