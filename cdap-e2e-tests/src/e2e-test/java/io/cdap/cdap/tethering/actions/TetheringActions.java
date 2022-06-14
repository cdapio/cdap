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

import io.cdap.cdap.tethering.locators.TetheringLocators;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.SeleniumHelper;

/**
 * Tethering related actions.
 */
public class TetheringActions {

  static {
    SeleniumHelper.getPropertiesLocators(TetheringLocators.class);
  }

  public static void openTetheringPage() {
    ElementHelper.clickOnElement(TetheringLocators.tetheringPage);
  }

  public static void clickMoreMenuPendingReq() {
    ElementHelper.clickOnElement(TetheringLocators.pendingReqMoreMenu);
  }

  public static void clickOnDeleteOptionPendingReq() {
    ElementHelper.clickOnElement(TetheringLocators.pendingReqDeleteOption);
  }

  public static void clickOnConfirmDelete() {
    ElementHelper.clickOnElement(TetheringLocators.deleteConfirmation);
  }

  public static int countNumberOfPendingClientReqs() {
    return ElementHelper.countNumberOfElements(TetheringLocators.pendingRequestLocatorClient);
  }

  public static int countNumberOfPendingServerReqs() {
    return ElementHelper.countNumberOfElements(TetheringLocators.pendingRequestLocatorServer);
  }

  public static void clickMoreMenuEstablishedConn() {
    ElementHelper.clickOnElement(TetheringLocators.establishedConnMoreMenu);
  }

  public static void clickOnDeleteOptionEstablishedConn() {
    ElementHelper.clickOnElement(TetheringLocators.establishedConnDeleteOption);
  }

  public static int countNumberOfEstablishedConns() {
    return ElementHelper.countNumberOfElements(TetheringLocators.establishedConnLocator);
  }

  public static void clickOnAcceptConnReq() {
    ElementHelper.clickOnElement(TetheringLocators.acceptConnReqButton);
  }

  public static void clickOnRejectConnReq() {
    ElementHelper.clickOnElement(TetheringLocators.rejectConnReqButton);
  }

  public static void clickOnConfirmReject() {
    ElementHelper.clickOnElement(TetheringLocators.rejectConfirmation);
  }
}
