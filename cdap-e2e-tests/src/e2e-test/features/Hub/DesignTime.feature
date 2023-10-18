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
Feature: Hub - Design time scenarios

  @TS-HUB-DESIGN-01
  Scenario: Verify that the user is able to successfully navigate to the Hub page
    When Open Datafusion Project to configure pipeline
    Then Click on Hub Menu
    Then Verify that user is navigated to hub page successfully
    Then Click on close button

  @TS-HUB-DESIGN-02
  Scenario: Verify that the user is getting an error message for an invalid search
    When Open Datafusion Project to configure pipeline
    Then Click on Hub Menu
    Then Enter the text in search tab "invalidMessage"
    Then Verify that user is getting an error message: "invalid.message_hub"
