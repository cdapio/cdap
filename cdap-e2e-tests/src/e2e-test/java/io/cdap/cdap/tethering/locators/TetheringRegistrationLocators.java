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

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * Tethering registration related locators.
 */
public class TetheringRegistrationLocators {

  @FindBy(how = How.CSS, using = "[data-testid='create-tethering-req-btn']")
  public static WebElement createNewReqButton;

  @FindBy(how = How.CSS, using = "[data-testid='projectName']")
  public static WebElement projectNameInput;

  @FindBy(how = How.CSS, using = "[data-testid='region']")
  public static WebElement regionInput;

  @FindBy(how = How.CSS, using = "[data-testid='instanceName']")
  public static WebElement instanceNameInput;

  @FindBy(how = How.CSS, using = "[data-testid='instanceUrl']")
  public static WebElement instanceUrlInput;

  @FindBy(how = How.CSS, using = "[data-testid='description']")
  public static WebElement descriptionInput;

  @FindBy(how = How.CSS, using = "[data-testid='tethering-req-accept-btn']")
  public static WebElement sendReqButton;

  @FindBy(how = How.CSS, using = "[data-testid='tethering-ns-chk-box']")
  public static WebElement namespaceCheckBox;

  @FindBy(how = How.CSS, using = "[data-testid='error']")
  public static WebElement reqErrorMessage;

  @FindBy(how = How.CSS, using = "[data-testid='no-ns-selected']")
  public static WebElement noNsErrorMessage;

  @FindBy(how = How.CSS, using = "[data-testid='missing-required-field']")
  public static WebElement missingReqFieldMessage;

  @FindBy(how = How.CSS, using = "[data-testid='success']")
  public static WebElement reqSuccessMessage;

}
