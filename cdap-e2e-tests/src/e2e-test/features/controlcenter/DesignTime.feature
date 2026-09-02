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

Feature: Controlcenter - Validate control center page flow design time features.

  Scenario: Verify user is able to click the control center tab and successfully navigates to control center page
    Given Open Datafusion Project to configure pipeline
    Then Click on the Hamburger bar on the left panel
    Then Click on Control Center link from the hamburger menu
    Then Verify that the user is navigated to control center page successfully

  Scenario: Verify the Display message should be updated on the basis of filter selection as per the user
    Given Open Datafusion Project to configure pipeline
    Then Click on the Hamburger bar on the left panel
    Then Click on Control Center link from the hamburger menu
    Then Select dropdown : "Filter" with option value: "Artifacts" in control center
    Then Verify the all entities message is displayed with the filter selection: "allEntitiesDisplayedMessage"

  Scenario: Verify User is able to switch between Schema and programs of the dataset.
    Given Open Datafusion Project to configure pipeline
    Then Click on the Hamburger bar on the left panel
    Then Click on Control Center link from the hamburger menu
    Then Verify the user is able to click dataset entity icon to navigate to details page
    Then Verify user is navigated to the details page of the dataset entity icon successfully
    Then Click on the schema link of the dataset entity details page
    Then Verify user is navigated to the schema details page of the dataset entity page
    Then Click on the programs link of the dataset entity details page
    Then Verify user is navigated to the programs details page of the dataset entity page

  Scenario: Verify User is able to sort the entities with all the available filter types in control center.
    Given Open Datafusion Project to configure pipeline
    Then Click on the Hamburger bar on the left panel
    Then Click on Control Center link from the hamburger menu
    Then Select the sort by dropdown with option value: "Newest"
    Then Verify the entities are sorted by the newest option: "newFilterMessage"
    Then Select the sort by dropdown with option value: "Oldest"
    Then Verify the entities are sorted by the oldest option: "oldestFilterMessage"
    Then Select the sort by dropdown with option value: "A - Z"
    Then Verify the entities are sorted by the Z to A option: "aToZFilterMessage"
    Then Select the sort by dropdown with option value: "Z - A"
    Then Verify the entities are sorted by the A to Z option: "zToAFilterMessage"

  Scenario: Verify the user is able to navigate to and from details page of a dataset inside control center
    Given Open Datafusion Project to configure pipeline
    Then Click on the Hamburger bar on the left panel
    Then Click on Control Center link from the hamburger menu
    Then Verify the user is able to click dataset entity icon to navigate to details page
    Then Click on view details tab of dataset entity
    Then Verify user is successfully navigated to details page of the dataset entity
    Then Click on the back link of the view details page of dataset entity
    Then Click on close link to close the details page of dataset entity
    Then Verify that the user is navigated to control center page successfully

  Scenario: Verify user should be able to search the dataset using the added tags.
    Given Open Datafusion Project to configure pipeline
    Then Click on the Hamburger bar on the left panel
    Then Click on Control Center link from the hamburger menu
    Then Verify the user is able to click dataset entity icon to navigate to details page
    Then Click on the plus button to add the tag for a dataset entity
    Then Verify user is able to enter the values in tag input field: "testingTag"
    Then Enter the text in search tab "testingTag" in control center
    Then Verify the searched tag is displayed successfully on control center page: "searchedTagDisplayedMessage"

  Scenario: Verify that User is able to click on the dataset entity and is navigated to the details page of the dataset successfully
    Given Open Datafusion Project to configure pipeline
    Then Click on the Hamburger bar on the left panel
    Then Click on Control Center link from the hamburger menu
    Then Verify the user is able to click dataset entity icon to navigate to details page
    Then Verify user is navigated to the details page of the dataset entity icon successfully

  Scenario: Verify that tags counts should be increased and decreased in case User perform Add or Remove actions on dataset entity.
    Given Open Datafusion Project to configure pipeline
    Then Click on the Hamburger bar on the left panel
    Then Click on Control Center link from the hamburger menu
    Then Verify the user is able to click dataset entity icon to navigate to details page
    Then Click on the plus button to add the tag for a dataset entity
    Then Verify user is able to enter the values in tag input field: "testingTag"
    Then Verify the tag count of dataset entity when user adds the tag
    Then Click on the close icon of tag added
    Then Verify the tag count of dataset entity decreases message: "tagCountDecreaseMessage"
    Then Click on close link to close the details page of dataset entity
