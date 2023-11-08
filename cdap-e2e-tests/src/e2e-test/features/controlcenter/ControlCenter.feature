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

@Controlcenter

Feature: Controlcenter - Validate control center page flow

  Scenario: Verify user is able to click the control center tab and successfully navigates to control center page
    Given Open Datafusion Project to configure pipeline
    Then Click on the Hamburger bar on the left panel
    Then Click on Control Center link from the hamburger menu
    Then Verify that the user is navigated to control center page successfully

  @BQ_INSERT_INT_SOURCE_TEST @BQ_SINK_TEST
  Scenario: Verify that user is able to create a pipeline and then validate the presence of created pipeline in control center.
    Given Open Datafusion Project to configure pipeline
    Then Click on the Hamburger bar on the left panel
    Then Click on Control Center link from the hamburger menu
    Then Click on the Plus Green Button to import the pipelines
    Then Verify user is able to click on the create button to create a pipeline successfully
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "projectId"
    And Replace input plugin property: "dataset" with value: "dataset"
    Then Override Service account details if set in environment variables
    And Enter input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Click on the Hamburger bar on the left panel
    Then Click on Control Center link from the hamburger menu
    Then Verify the pipeline created successfully is present in control center page

   @BQ_INSERT_INT_SOURCE_TEST @BQ_SINK_TEST
    Scenario: Verify that user is able to delete the created pipeline in control center successfully.
     Given Open Datafusion Project to configure pipeline
     Then Click on the Hamburger bar on the left panel
     Then Click on Control Center link from the hamburger menu
     Then Click on the Plus Green Button to import the pipelines
     Then Verify user is able to click on the create button to create a pipeline successfully
     When Expand Plugin group in the LHS plugins list: "Source"
     When Select plugin: "BigQuery" from the plugins list as: "Source"
     When Expand Plugin group in the LHS plugins list: "Sink"
     When Select plugin: "BigQuery" from the plugins list as: "Sink"
     Then Connect plugins: "BigQuery" and "BigQuery2" to establish connection
     Then Navigate to the properties page of plugin: "BigQuery"
     And Enter input plugin property: "referenceName" with value: "Reference"
     And Replace input plugin property: "project" with value: "projectId"
     And Enter input plugin property: "datasetProject" with value: "projectId"
     And Replace input plugin property: "dataset" with value: "dataset"
     Then Override Service account details if set in environment variables
     And Enter input plugin property: "table" with value: "bqSourceTable"
     Then Click on the Get Schema button
     Then Validate "BigQuery" plugin properties
     And Close the Plugin Properties page
     Then Navigate to the properties page of plugin: "BigQuery2"
     Then Replace input plugin property: "project" with value: "projectId"
     Then Override Service account details if set in environment variables
     Then Enter input plugin property: "datasetProject" with value: "projectId"
     Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
     Then Enter input plugin property: "dataset" with value: "dataset"
     Then Enter input plugin property: "table" with value: "bqTargetTable"
     Then Validate "BigQuery" plugin properties
     And Close the Plugin Properties page
     Then Save the pipeline
     Then Deploy the pipeline
     Then Click on the Hamburger bar on the left panel
     Then Click on Control Center link from the hamburger menu
     Then Click on the delete icon of the created pipeline and pipeline should get deleted successfully
     Then Verify the deleted pipeline is not present in the control center page

     @BQ_INSERT_INT_SOURCE_TEST @BQ_SINK_TEST
     Scenario: Verify User is able to create pipeline by using preferences,entering the key and value pair and validate it deploy successfully.
       Given Open Datafusion Project to configure pipeline
       Then Click on the Hamburger bar on the left panel
       Then Click on Control Center link from the hamburger menu
       Then Click on the Plus Green Button to import the pipelines
       Then Verify user is able to click on the create button to create a pipeline successfully
       When Expand Plugin group in the LHS plugins list: "Source"
       When Select plugin: "BigQuery" from the plugins list as: "Source"
       When Expand Plugin group in the LHS plugins list: "Sink"
       When Select plugin: "BigQuery" from the plugins list as: "Sink"
       Then Connect plugins: "BigQuery" and "BigQuery2" to establish connection
       Then Navigate to the properties page of plugin: "BigQuery"
       And Enter input plugin property: "referenceName" with value: "Reference"
       Then Click on the Macro button of Property: "project" and set the value to: "projectId"
       And Enter input plugin property: "datasetProject" with value: "projectId"
       And Replace input plugin property: "dataset" with value: "dataset"
       Then Override Service account details if set in environment variables
       And Enter input plugin property: "table" with value: "bqSourceTable"
       Then Click on the Get Schema button
       Then Validate "BigQuery" plugin properties
       And Close the Plugin Properties page
       Then Navigate to the properties page of plugin: "BigQuery2"
       Then Click on the Macro button of Property: "project" and set the value to: "projectId"
       Then Override Service account details if set in environment variables
       Then Enter input plugin property: "datasetProject" with value: "projectId"
       Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
       Then Enter input plugin property: "dataset" with value: "dataset"
       Then Enter input plugin property: "table" with value: "bqTargetTable"
       Then Validate "BigQuery" plugin properties
       And Close the Plugin Properties page
       Then Save the pipeline
       Then Deploy the pipeline
       Then Click on the Hamburger bar on the left panel
       Then Click on Control Center link from the hamburger menu
       Then Verify the user is able to set the preferences for the created pipeline in the control center page
       Then Verify the user is able to enter the value in the key input field "keyValue"
       Then Verify the user is able to enter the value of the key in the value input field "value"
       Then Verify user is able to click on save and close button of set preferences
       Then Verify user is able to click on the data pipeline added in the control center page
       Then Enter runtime argument value "projectId" for key "projectId"
       Then Run the Pipeline in Runtime with runtime arguments
       Then Wait till pipeline is in running state
       Then Verify the pipeline status is "Succeeded"
