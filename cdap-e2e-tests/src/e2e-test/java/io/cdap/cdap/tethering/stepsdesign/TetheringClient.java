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

package io.cdap.cdap.tethering.stepsdesign;

import io.cdap.cdap.tethering.actions.TetheringActions;
import io.cdap.e2e.pages.actions.CdfSysAdminActions;
import io.cdap.e2e.pages.actions.HdfSignInActions;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.PageHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.io.IOException;

/**
 * Tethering client management related steps definitions.
 */
public class TetheringClient implements CdfHelper {

  private static final int WAIT_TIME_MS = 3000;
  private static int pendingReqCount;
  private static int establishedConnCount;

  @Given("Open tethering client Datafusion instance")
  public static void openTetheringClientInstance() throws IOException, InterruptedException {
    SeleniumDriver.openPage(PluginPropertyUtils.pluginProp("clientUrl"));
    PageHelper.acceptAlertIfPresent();
    if (!HdfSignInActions.logged()) {
      HdfSignInActions.login();
      PageHelper.acceptAlertIfPresent();
    }
  }

  @When("Navigate to tethering page")
  public static void navigateToTetheringPage() throws InterruptedException {
    CdfSysAdminActions.clickSystemAdminMenu();
    TetheringActions.openTetheringPage();
    Thread.sleep(WAIT_TIME_MS);
  }

  @Then("Count number of pending requests on client")
  public static void countNumberOfPendingReqs() {
    pendingReqCount = TetheringActions.countNumberOfPendingClientReqs();
  }

  @Then("Click on the more menu of a pending request")
  public static void clickOnMoreMenuPendingReq() {
    TetheringActions.clickMoreMenuPendingReq();
  }

  @Then("Click on Delete option for pending request")
  public static void clickDeleteOptionPendingReq() {
    TetheringActions.clickOnDeleteOptionPendingReq();
  }

  @Then("Confirm the delete action")
  public static void confirmDeleteAction() {
    TetheringActions.clickOnConfirmDelete();
  }

  @Then("Verify the pending request has been deleted")
  public static void verifyDeletionOfPendingRequest() throws Exception {
    Thread.sleep(WAIT_TIME_MS);
    int updatedPendingReqCount = TetheringActions.countNumberOfPendingClientReqs();
    if (updatedPendingReqCount != pendingReqCount - 1) {
      throw new Exception("Failure in pending request deletion.");
    }
    pendingReqCount = updatedPendingReqCount;
  }

  @Then("Count number of established connections on client")
  public static void countNumberOfEstablishedConns() {
    establishedConnCount = TetheringActions.countNumberOfEstablishedConns();
  }

  @Then("Verify the connection is established")
  public static void verifyEstablishedConnection() throws Exception {
    Thread.sleep(WAIT_TIME_MS);
    PageHelper.refreshCurrentPage();
    int updatedPendingReqCount = TetheringActions.countNumberOfPendingClientReqs();
    int updatedEstablishedConnCount = TetheringActions.countNumberOfEstablishedConns();
    if (updatedPendingReqCount != pendingReqCount - 1 && updatedEstablishedConnCount != establishedConnCount + 1) {
      throw new Exception("Failure in creating an established connection.");
    }
    pendingReqCount = updatedPendingReqCount;
    establishedConnCount = updatedEstablishedConnCount;
  }

  @Then("Click on the more menu of a established connection")
  public static void clickOnMoreMenuEstablishedConn() {
    TetheringActions.clickMoreMenuEstablishedConn();
  }

  @Then("Click on Delete option for established connection")
  public static void clickDeleteOptionEstablishedConn() {
    TetheringActions.clickOnDeleteOptionEstablishedConn();
  }

  @Then("Verify the established connection has been deleted on client")
  public static void verifyDeletionOfEstablishedConn() throws Exception {
    Thread.sleep(WAIT_TIME_MS);
    int updatedEstablishedConnCount = TetheringActions.countNumberOfEstablishedConns();
    if (updatedEstablishedConnCount != establishedConnCount - 1) {
      throw new Exception("Failure in established connection deletion.");
    }
    establishedConnCount = updatedEstablishedConnCount;
  }
}
