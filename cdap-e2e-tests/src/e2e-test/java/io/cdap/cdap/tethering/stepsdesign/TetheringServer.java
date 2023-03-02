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

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import io.cdap.cdap.tethering.actions.TetheringActions;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * Tethering server management related steps definitions.
 */
public class TetheringServer implements CdfHelper {
  private static final Gson GSON = new Gson();

  private static final int WAIT_TIME_MS = 3000;
  private static int pendingReqCount;
  private static int establishedConnCount;

  @Given("Connect to tethering server Datafusion instance")
  public static void openTetheringServerInstance() throws IOException {
    HttpURLConnection connection = createConnection("tethering/connections", "GET");
    try {
      verifyConnection(connection);
    } finally {
      connection.disconnect();
    }
  }

  @Then("Reject request on server from client")
  public static void rejectRequest() throws IOException {
    String clientName = PluginPropertyUtils.pluginProp("clientName");
    HttpURLConnection connection = createConnection("tethering/connections/" + clientName, "POST");
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);
    try {
      connection.getOutputStream().write("{\"action\":\"reject\"}".getBytes(StandardCharsets.UTF_8));
      verifyConnection(connection);
    } finally {
      connection.disconnect();
    }
  }

  @Then("Accept request on server from client")
  public static void acceptRequest() throws IOException {
    String clientName = PluginPropertyUtils.pluginProp("clientName");
    HttpURLConnection connection = createConnection("tethering/connections/" + clientName, "POST");
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);
    try {
      connection.getOutputStream().write("{\"action\":\"accept\"}".getBytes(StandardCharsets.UTF_8));
      verifyConnection(connection);
    } finally {
      connection.disconnect();
    }
  }

  @Then("Verify no pending tethering requests on server")
  public static void verifyNoRequests() throws IOException {
    HttpURLConnection connection = createConnection("tethering/connections/", "GET");
    try {
      verifyConnection(connection);
      String responseOutput = new String(ByteStreams.toByteArray(connection.getInputStream()), Charsets.UTF_8);
      if (GSON.toJson(responseOutput).contains("PENDING")) {
        throw new IOException(String.format("Expected no pending requests remaining but received %s", responseOutput));
      }
    } finally {
      connection.disconnect();
    }
  }

  @Then("Delete tethering connection on server")
  public static void deleteTethering() throws IOException {
    String clientName = PluginPropertyUtils.pluginProp("clientName");
    HttpURLConnection connection = createConnection("tethering/connections/" + clientName, "DELETE");
    try {
      verifyConnection(connection);
    } finally {
      connection.disconnect();
    }
  }

  @Then("Count number of pending requests on server")
  public static void countNumberOfPendingReqsServer() {
    pendingReqCount = TetheringActions.countNumberOfPendingServerReqs();
  }

  @Then("Count number of established connections on server")
  public static void countNumberOfEstablishedConnsServer() {
    establishedConnCount = TetheringActions.countNumberOfEstablishedConns();
  }

  @Then("Click on accept button")
  public static void clickAcceptButton() throws InterruptedException {
    TetheringActions.clickOnAcceptConnReq();
    Thread.sleep(WAIT_TIME_MS);
  }

  @Then("Click on reject button")
  public static void clickRejectButton() {
    TetheringActions.clickOnRejectConnReq();
  }

  @Then("Confirm the reject action")
  public static void confirmRejectAction() {
    TetheringActions.clickOnConfirmReject();
  }

  @Then("Verify the request has been accepted")
  public static void verifyAcceptanceOfConnRequest() throws Exception {
    int updatedPendingReqCount = TetheringActions.countNumberOfPendingServerReqs();
    int updatedEstablishedConnCount = TetheringActions.countNumberOfEstablishedConns();
    if (updatedPendingReqCount != pendingReqCount - 1 && updatedEstablishedConnCount != establishedConnCount + 1) {
      throw new Exception("Failure in accepting connection request.");
    }
    pendingReqCount = updatedPendingReqCount;
    establishedConnCount = updatedEstablishedConnCount;
  }

  @Then("Verify the request has been rejected")
  public static void verifyRejectionOfConnRequest() throws Exception {
    int updatedPendingReqCount = TetheringActions.countNumberOfPendingServerReqs();
    if (updatedPendingReqCount != pendingReqCount - 1) {
      throw new Exception("Failure in rejecting connection request.");
    }
  }

  @Then("Verify the established connection has been deleted on server")
  public static void verifyDeletionOfEstablishedConn() throws Exception {
    int updatedEstablishedConnCount = TetheringActions.countNumberOfEstablishedConns();
    if (updatedEstablishedConnCount != establishedConnCount - 1) {
      throw new Exception("Failure in pending request deletion.");
    }
  }

  @Then("Create compute profile to tethered client")
  public static void createProfile() throws IOException {
    String clientName = PluginPropertyUtils.pluginProp("clientName");
    HttpURLConnection connection = createConnection("profiles/tether-profile", "PUT");
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);
    try {
      String contents = String.format("{\"label\": \"tethering-profile\",\"description\": \"test profile\","
                                        + "\"provisioner\": {\"name\": \"tethering\",\"properties\": [{\"name\": "
                                        + "\"tetheredInstanceName\",\"value\": \"%s\"},{\"name\": "
                                        + "\"tetheredNamespace\",\"value\": \"default\"}]}}",
                                      clientName);
      connection.getOutputStream().write(contents.getBytes(StandardCharsets.UTF_8));
      verifyConnection(connection);
    } finally {
      connection.disconnect();
    }
  }

  @Then("Verify compute profile was created successfully")
  public static void verifyProfile() throws IOException {
    HttpURLConnection connection = createConnection("profiles/tether-profile", "GET");
    try {
      verifyConnection(connection);
    } finally {
      connection.disconnect();
    }
  }

  @Then("Delete compute profile")
  public static void deleteProfile() throws IOException {
    HttpURLConnection connection = createConnection("profiles/tether-profile/disable", "POST");
    connection.setDoOutput(true);
    try {
      connection.getOutputStream().write("".getBytes(StandardCharsets.UTF_8));
      verifyConnection(connection);
      connection = createConnection("profiles/tether-profile", "DELETE");
      verifyConnection(connection);
    } finally {
      connection.disconnect();
    }
  }

  private static HttpURLConnection createConnection(String path, String method) throws IOException {
    String serverUrl = PluginPropertyUtils.pluginProp("serverUrl");
    String accessToken = PluginPropertyUtils.pluginProp("serverAccessToken");
    URL url = new URL(serverUrl + "/v3/" + path);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestProperty("Authorization", "Bearer " + accessToken);
    connection.setRequestMethod(method);
    return connection;
  }

  private static void verifyConnection(HttpURLConnection connection) throws IOException {
    if (connection.getResponseCode() != 200) {
      String msg = String.format("Received response code %s and error: %s", connection.getResponseCode(),
                                 new String(ByteStreams.toByteArray(connection.getErrorStream()), Charsets.UTF_8));
      throw new IOException(msg);
    }
  }
}


