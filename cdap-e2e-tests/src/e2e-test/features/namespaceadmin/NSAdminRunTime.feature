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
Feature: NameSpaceAdmin - Validate nameSpace admin run time scenarios

  @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario:To verify if user is able to run a pipeline successfully using the namespace preferences
    Given Open Datafusion Project to configure pipeline
    Then Click on the Hamburger bar on the left panel
    Then Click on NameSpace Admin link from the menu
    Then Click "preferences" tab from Configuration page for "default" Namespace
    Then Click on edit namespace preferences to set namespace preferences
    Then Set namespace preferences with key: "keyValue" and value: "nameSpacePreferences2"
    Then Click on the Save & Close preferences button
    Then Click on the Hamburger bar on the left panel
    Then Select navigation item: "studio" from the Hamburger menu list
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Click on the Macro button of Property: "projectId" and set the value to: "projectId"
    Then Click on the Macro button of Property: "datasetProjectId" and set the value to: "datasetprojectId"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Click on the Macro button of Property: "projectId" and set the value to: "projectId"
    Then Click on the Macro button of Property: "datasetProjectId" and set the value to: "datasetprojectId"
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

  @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario: To verify if user is able to create a connection from namespace admin and configure it for required plugins
    Given Open Datafusion Project to configure pipeline
    Then Click on the Hamburger bar on the left panel
    Then Click on NameSpace Admin link from the menu
    Then Click "connections" tab from Configuration page for "default" Namespace
    Then Click on the Add Connection button
    Then Add connection type as "bqConnection" and provide a "ConnectionName"
    Then Click on the Test Connection button
    Then Click on the Create button
    Then Click on the Hamburger bar on the left panel
    Then Select navigation item: "studio" from the Hamburger menu list
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Click plugin property: "switch-useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "ConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Click on the Browse button inside plugin properties
    Then Click SELECT button inside connection data row with name: "dataset"
    Then Wait till connection data loading completes with a timeout of 60 seconds
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "ConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQSinkReferenceName"
    Then Click on the Browse button inside plugin properties
    Then Click SELECT button inside connection data row with name: "dataset"
    Then Wait till connection data loading completes with a timeout of 60 seconds
    Then Verify input plugin property: "dataset" contains value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Click plugin property: "truncateTable"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
