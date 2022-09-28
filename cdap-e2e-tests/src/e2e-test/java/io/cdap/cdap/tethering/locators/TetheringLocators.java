/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.tethering.locators;

import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * Tethering related locators.
 */
public class TetheringLocators {

  @FindBy(how = How.XPATH, using = "//a[@href='/cdap/administration/tethering']")
  public static WebElement tetheringPage;

  @FindBy(how = How.CSS, using = "[data-testid='tethering-request']")
  public static WebElement pendingReqMoreMenu;

  @FindBy(how = How.CSS, using = "[data-testid='delete-tethering-request']")
  public static WebElement pendingReqDeleteOption;

  @FindBy(how = How.CSS, using = "[data-testid='Delete']")
  public static WebElement deleteConfirmation;

  public static By pendingRequestLocatorClient = By.cssSelector("[data-testid='tethering-request']");

  public static By pendingRequestLocatorServer = By.cssSelector("[data-testid='accept-connection']");

  @FindBy(how = How.CSS, using = "[data-testid='established-connection']")
  public static WebElement establishedConnMoreMenu;

  @FindBy(how = How.CSS, using = "[data-testid='delete-connection']")
  public static WebElement establishedConnDeleteOption;

  public static By establishedConnLocator = By.cssSelector("[data-testid='established-connection']");

  @FindBy(how = How.CSS, using = "[data-testid='accept-connection']")
  public static WebElement acceptConnReqButton;

  @FindBy(how = How.CSS, using = "[data-testid='reject-connection']")
  public static WebElement rejectConnReqButton;

  @FindBy(how = How.CSS, using = "[data-testid='Reject']")
  public static WebElement rejectConfirmation;
}
