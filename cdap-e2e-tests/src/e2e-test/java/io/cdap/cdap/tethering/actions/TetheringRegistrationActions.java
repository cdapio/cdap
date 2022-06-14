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

package io.cdap.cdap.tethering.actions;

import io.cdap.cdap.tethering.locators.TetheringRegistrationLocators;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.SeleniumHelper;

/**
 * Tethering registration related actions.
 */
public class TetheringRegistrationActions {

  static {
    SeleniumHelper.getPropertiesLocators(TetheringRegistrationLocators.class);
  }

  public static void openTetheringRegistrationPage() {
    ElementHelper.clickOnElement(TetheringRegistrationLocators.createNewReqButton);
  }

  public static void clickSendReqButton() {
    ElementHelper.clickOnElement(TetheringRegistrationLocators.sendReqButton);
  }

  public static void clickNamespaceCheckbox() {
    ElementHelper.clickOnElement(TetheringRegistrationLocators.namespaceCheckBox);
  }

  public static void enterProjectName(String projName) {
    ElementHelper.replaceElementValue(TetheringRegistrationLocators.projectNameInput, projName);
  }

  public static void enterRegion(String region) {
    ElementHelper.replaceElementValue(TetheringRegistrationLocators.regionInput, region);
  }

  public static void enterInstanceName(String instanceName) {
    ElementHelper.replaceElementValue(TetheringRegistrationLocators.instanceNameInput, instanceName);
  }

  public static void enterInstanceUrl(String instanceUrl) {
    ElementHelper.replaceElementValue(TetheringRegistrationLocators.instanceUrlInput, instanceUrl);
  }

  public static void enterDescription(String description) {
    ElementHelper.replaceElementValue(TetheringRegistrationLocators.descriptionInput, description);
  }

  public static boolean isReqCreationSucceeded() {
    return ElementHelper.isElementDisplayed(TetheringRegistrationLocators.reqSuccessMessage);
  }

  public static boolean isReqCreationFailed() {
    return ElementHelper.isElementDisplayed(TetheringRegistrationLocators.reqErrorMessage);
  }

  public static boolean isReqCreationFailedWithNoNs() {
    return ElementHelper.isElementDisplayed(TetheringRegistrationLocators.noNsErrorMessage);
  }

  public static boolean isReqCreationFailedWithMissingRequiredField() {
    return ElementHelper.isElementDisplayed(TetheringRegistrationLocators.missingReqFieldMessage);
  }

}
