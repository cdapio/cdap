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

@Hub
Feature: Hub - Run time scenarios

  @TS-HUB-RNTM-01
  Scenario: Verify that the user is able to deploy a plugin from Hub and then delete it successfully
    When Open Datafusion Project to configure pipeline
    Then Click on Hub Menu
    Then Click on "Plugins" option
    Then Enter the text in search tab "Anaplan"
    Then Click on "Anaplan plugins" plugin
    Then Deploy the plugin
    Then Click on "Go to homepage" button
    Then Verify that "Anaplan" plugin is successfully deployed
    Then Enter the text in search tab "anaplan" in control center
    Then Select dropdown : "Filter" with option value: "Artifacts" in control center
    Then Click on delete button to delete the plugin
    Then Verify that plugin is successfully deleted
