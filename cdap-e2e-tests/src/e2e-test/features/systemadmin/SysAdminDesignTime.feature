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

@Sysadmin
Feature: Sysadmin - Validate system admin page design time scenarios

  Background:
    Given Open Datafusion Project to configure pipeline
    When Open "System Admin" menu
    Then Click on the Configuration link on the System admin page

  @Sysadmin @SysAdminRequired
  Scenario:Validate user is able to create new system preferences and able to delete the added system preferences successfully
    Then Select "systemPreferences" option from Configuration page
    Then Click on edit system preferences
    Then Set system preferences with key: "keyValue" and value: "systemPreferences1"
    Then Click on the Save & Close preferences button
    Then Select "systemPreferences" option from Configuration page
    Then Click on edit system preferences
    Then Delete the preferences
    Then Click on the Save & Close preferences button
    Then Verify the system admin page is navigated successfully

  Scenario:Validate user is able to add multiple system preferences inside system admin successfully
    Then Select "systemPreferences" option from Configuration page
    Then Click on edit system preferences
    Then Set system preferences with key: "keyValue" and value: "systemPreferences2"
    Then Click on the Save & Close preferences button
    Then Click on edit system preferences
    Then Delete the preferences
    Then Delete the preferences
    Then Click on the Save & Close preferences button
    Then Verify the system admin page is navigated successfully

  Scenario:Validate user is able to successfully reload system artifacts using reload
    Then Click on Reload System Artifacts from the System admin page
    Then Click on Reload button on popup to reload the System Artifacts successfully
    Then Verify the system admin page is navigated successfully

  Scenario:Validate user is able to open compute profile page and create a compute profile for selected provisioner
    Then Click on the Compute Profile from the System admin page
    Then Click on create compute profile button
    Then Select a provisioner: "remoteHadoopProvisioner" for the compute profile
    Then Verify the Create a Profile page is loaded for selected provisioner
    Then Enter input plugin property: "profileLabel" with value: "validProfile"
    Then Enter textarea plugin property: "profileDescription" with value: "validDescription"
    Then Enter input plugin property: "host" with value: "testHost"
    Then Enter input plugin property: "user" with value: "testUser"
    Then Enter textarea plugin property: "sshKey" with value: "testSSHKey"
    Then Click on: "Create" button in the properties
    Then Verify the created compute profile: "validProfile" is displayed in system compute profile list
