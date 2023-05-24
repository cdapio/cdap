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

@Integration_Tests
Feature: Test operational stats

  @OPERATIONAL_STATS_TEST
  Scenario: Correct operational stats should be shown in UI
    When Open administration page
    Then Check system uptime is positive
    And Check in table "generic-details-entities" value "Namespaces" is greater than or equal to 1
    And Check in table "generic-details-entities" value "Artifacts" is greater than or equal to 0
    And Check in table "generic-details-entities" value "Applications" is greater than or equal to 0
    And Check in table "generic-details-entities" value "Programs" is greater than or equal to 0
    And Check in table "generic-details-entities" value "Datasets" is greater than or equal to 0

    And Check in table "generic-details-lasthourload" value "TotalRequests" is greater than or equal to 0
    And Check in table "generic-details-lasthourload" value "Successful" is greater than or equal to 0
    And Check in table "generic-details-lasthourload" value "ServerErrors" is greater than or equal to 0
    And Check in table "generic-details-lasthourload" value "ClientErrors" is greater than or equal to 0
    And Check in table "generic-details-lasthourload" value "WarnLogs" is greater than or equal to 0
    And Check in table "generic-details-lasthourload" value "ErrorLogs" is greater than or equal to 0

    And Check in table "generic-details-transactions" value "NumCommittingChangeSets" is greater than or equal to 0
    And Check in table "generic-details-transactions" value "NumCommittedChangeSets" is greater than or equal to 0
    And Check in table "generic-details-transactions" value "NumInProgressTransactions" is greater than or equal to 0
    And Check in table "generic-details-transactions" value "NumInvalidTransactions" is greater than or equal to 0
