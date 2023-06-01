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

package io.cdap.cdap.operations.cdap.utils;

import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.WaitHelper;

import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

import org.junit.Assert;
import io.cdap.cdap.operations.cdap.utils.Constants;

/**
 * Operational Stats plugin related Actions.
 */

public class OperationalStatsActions {

  public static void openAdminPage() {
    SeleniumDriver.openPage(Constants.ADMIN_PAGE_URL);
    WaitHelper.waitForPageToLoad();
  }

  public static void checkUptimeValue() {
    String xpath = "//span[@data-testid='system-uptime-container']";
    WebElement el = SeleniumDriver.getDriver().findElement(By.xpath(xpath));
    double uptime = Double.parseDouble(el.getAttribute("data-uptime"));
    Assert.assertTrue(uptime > 0);
  }

  public static void checkValueInTableGTE(String tableTestId, String key, int lowerLimit) {
    Assert.assertTrue(findValueInGenericTable(tableTestId, key) >= lowerLimit);
  }

  public static double findValueInGenericTable(String tableTestId, String key) {
    String xpath = "//div[@data-testid='" + tableTestId + "']//td[@data-testid='" + key + "']";
    WebElement el = SeleniumDriver.getDriver().findElement(By.xpath(xpath));
    return Double.parseDouble(el.getText().replaceAll(",", ""));
  }

}
