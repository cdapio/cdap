/*
 * Copyright © 2023 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.operations.cdap.stepdesign;

import io.cdap.e2e.utils.CdfHelper;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.cdap.cdap.operations.cdap.utils.OperationalStatsActions;

/**
 *Operational Stats plugin related StepsDesign.
 */

public class OperationalStatsSteps implements CdfHelper {

  @When("Open administration page")
  public void openAdminPage() {
    OperationalStatsActions.openAdminPage();
  }

  @Then("Check system uptime is positive")
  public void checkUptime() {
    OperationalStatsActions.checkUptimeValue();
  }

  @Then("Check in table {string} value {string} is greater than or equal to {int}")
  public void checkInTableValueIsGTE(String tableTestId, String key, int lowerLimit) {
    OperationalStatsActions.checkValueInTableGTE(tableTestId, key, lowerLimit);
  }

}
