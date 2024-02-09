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

@Sysadmin  @ignore
Feature: Sysadmin - Validate system admin page Run time scenarios

  Background:
    Given Open Datafusion Project to configure pipeline
    When Open "System Admin" menu
    Then Click on the Configuration link on the System admin page

  @Sysadmin
  Scenario:To verify user should be able to create Namespace successfully in System Admin
    Then Click on Create New Namespace button
    Then Enter the New Namespace Name with value: "namespaceName"
    Then Enter the Namespace Description with value: "validNamespaceDescription"
    Then Click on: "Finish" button in the properties
    Then Verify the namespace created success message displayed on confirmation window
    Then Verify the created namespace: "namespaceName" is displayed in Namespace tab

  @SysAdminRequired
  Scenario:To verify User should be able to add a secure key from Make HTTP calls successfully with PUT calls
    Then Click on Make HTTP calls from the System admin configuration page
    Then Select request dropdown property with option value: "httpPutMethod"
    Then Enter input plugin property: "requestPath" with value: "secureKey"
    Then Enter textarea plugin property: "requestBody" with value: "bodyValue"
    Then Click on send button
    Then Verify the status code for success response

  @SysAdminRequired
  Scenario:To verify User should be able to fetch secure key from Make HTTP calls successfully with GET calls
    Then Click on Make HTTP calls from the System admin configuration page
    Then Select request dropdown property with option value: "httpGetMethod"
    Then Enter input plugin property: "requestPath" with value: "secureKey"
    Then Click on send button
    Then Verify the status code for success response

  @SysAdminRequired
  Scenario:To verify User should be able to delete secure key from Make HTTP calls successfully with DELETE calls
    Then Click on Make HTTP calls from the System admin configuration page
    Then Select request dropdown property with option value: "httpDeleteMethod"
    Then Enter input plugin property: "requestPath" with value: "secureKey"
    Then Click on send button
    Then Verify the status code for success response

  @BQ_SOURCE_TEST @BQ_SINK_TEST @SysAdminRequired
  Scenario:To verify user should be able to run a pipeline successfully using the System preferences created
    Then Select "systemPreferences" option from Configuration page
    Then Click on edit system preferences
    Then Set system preferences with key: "keyValue" and value: "systemPreferences2"
    Then Click on the Save & Close preferences button
    Then Click on the Hamburger menu on the left panel
    Then Select navigation item: "studio" from the Hamburger menu list
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Click on the Macro button of Property: "projectId" and set the value to: "projectId"
    Then Click on the Macro button of Property: "datasetProjectId" and set the value to: "datasetProjectId"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Click on the Macro button of Property: "projectId" and set the value to: "projectId"
    Then Click on the Macro button of Property: "datasetProjectId" and set the value to: "datasetProjectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"

  @BQ_SOURCE_TEST @BQ_SINK_TEST @SysAdminRequired
  Scenario:To verify user should be able to run a pipeline successfully using existing System preferences and the Namespace preferences created
    Then Click on Create New Namespace button
    Then Enter the New Namespace Name with value: "sampleNamespaceName"
    Then Enter the Namespace Description with value: "validNamespaceDescription"
    Then Click on: "Finish" button in the properties
    Then Verify the namespace created success message displayed on confirmation window
    Then Click on the switch to namespace button
    Then Click on the Hamburger menu on the left panel
    Then Select navigation item: "namespaceAdmin" from the Hamburger menu list
    Then Click "preferences" tab from Configuration page for "sampleNamespaceName" Namespace
    Then Click on edit namespace preferences to set namespace preferences
    Then Set system preferences with key: "keyValue" and value: "systemPreferences1"
    Then Click on the Save & Close preferences button
    Then Click on the Hamburger menu on the left panel
    Then Select navigation item: "studio" from the Hamburger menu list
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Click on the Macro button of Property: "projectId" and set the value to: "projectId"
    Then Click on the Macro button of Property: "datasetProjectId" and set the value to: "datasetProjectId"
    Then Click on the Macro button of Property: "dataset" and set the value to: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Click on the Macro button of Property: "projectId" and set the value to: "projectId"
    Then Click on the Macro button of Property: "datasetProjectId" and set the value to: "datasetProjectId"
    Then Click on the Macro button of Property: "dataset" and set the value to: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    When Open "System Admin" menu
    Then Click on the Configuration link on the System admin page
    Then Select "systemPreferences" option from Configuration page
    Then Click on edit system preferences
    Then Delete the preferences
    Then Delete the preferences
    Then Click on the Save & Close preferences button
    Then Click on the Hamburger menu on the left panel
    Then Select navigation item: "namespaceAdmin" from the Hamburger menu list
    Then Click "preferences" tab from Configuration page for "sampleNamespaceName" Namespace
    Then Click on edit namespace preferences to set namespace preferences
    Then Delete the preferences
    Then Click on the Save & Close preferences button
