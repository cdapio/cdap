/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.examples.purchase;

import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.RuntimeMetrics;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Test for {@link PurchaseApp}.
 */
public class PurchaseAppTest extends TestBase {

  private static final Gson GSON = new Gson();

  @Test
  public void test() throws TimeoutException, InterruptedException, IOException {
    // Deploy the PurchaseApp application
    ApplicationManager appManager = deployApplication(PurchaseApp.class);

    // Start PurchaseFlow
    FlowManager flowManager = appManager.startFlow("PurchaseFlow");

    // Send stream events to the "purchaseStream" Stream
    StreamWriter streamWriter = appManager.getStreamWriter("purchaseStream");
    streamWriter.send("bob bought 3 apples for $30");
    streamWriter.send("joe bought 1 apple for $100");
    streamWriter.send("joe bought 10 pineapples for $20");
    streamWriter.send("cat bought 3 bottles for $12");
    streamWriter.send("cat bought 2 pops for $14");

    try {
      // Wait for the last Flowlet processing 5 events, or at most 5 seconds
      RuntimeMetrics metrics = RuntimeStats.getFlowletMetrics("PurchaseHistory", "PurchaseFlow", "collector");
      metrics.waitForProcessed(5, 15, TimeUnit.SECONDS);
    } finally {
      flowManager.stop();
    }

    ServiceManager userProfileServiceManager = getServiceManager(appManager);

    // Add customer's profile information
    URL userProfileUrl = new URL(userProfileServiceManager.getServiceURL(15, TimeUnit.SECONDS),
                                    UserProfileServiceHandler.USER_ENDPOINT);
    HttpURLConnection userProfileConnection = (HttpURLConnection) userProfileUrl.openConnection();
    String userProfileJson = "{'id' : 'joe', 'firstName': 'joe', 'lastName':'bernard', 'categories': ['fruits']}";

    try {
      userProfileConnection.setDoOutput(true);
      userProfileConnection.setRequestMethod("POST");
      userProfileConnection.getOutputStream().write(userProfileJson.getBytes(Charsets.UTF_8));
      Assert.assertEquals(HttpURLConnection.HTTP_OK, userProfileConnection.getResponseCode());
    } finally {
      userProfileConnection.disconnect();
    }

    // Test service to retrieve customer's profile information
    userProfileUrl = new URL(userProfileServiceManager.getServiceURL(15, TimeUnit.SECONDS),
                                    UserProfileServiceHandler.USER_ENDPOINT + "/joe");
    userProfileConnection = (HttpURLConnection) userProfileUrl.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, userProfileConnection.getResponseCode());
    String customerJson;
    try {
      customerJson = new String(ByteStreams.toByteArray(userProfileConnection.getInputStream()), Charsets.UTF_8);
    } finally {
      userProfileConnection.disconnect();
    }

    UserProfile profileFromService = GSON.fromJson(customerJson, UserProfile.class);
    Assert.assertEquals(profileFromService.getFirstName(), "joe");
    Assert.assertEquals(profileFromService.getLastName(), "bernard");

    // Run PurchaseHistoryWorkflow which will process the data
    MapReduceManager mapReduceManager = appManager.startMapReduce("PurchaseHistoryBuilder",
                                                                  ImmutableMap.<String, String>of());
    mapReduceManager.waitForFinish(3, TimeUnit.MINUTES);

    // Start PurchaseHistoryService
    ServiceManager purchaseHistoryServiceManager = appManager.startService(PurchaseHistoryService.SERVICE_NAME);

    // Wait for service startup
    serviceStatusCheck(purchaseHistoryServiceManager, true);

    // Test service to retrieve a customer's purchase history
    URL url = new URL(purchaseHistoryServiceManager.getServiceURL(15, TimeUnit.SECONDS), "history/joe");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    String historyJson;
    try {
      historyJson = new String(ByteStreams.toByteArray(conn.getInputStream()), Charsets.UTF_8);
    } finally {
      conn.disconnect();
    }
    PurchaseHistory history = GSON.fromJson(historyJson, PurchaseHistory.class);
    Assert.assertEquals("joe", history.getCustomer());
    Assert.assertEquals(2, history.getPurchases().size());

    UserProfile profileFromPurchaseHistory = history.getUserProfile();
    Assert.assertEquals(profileFromPurchaseHistory.getFirstName(), "joe");
    Assert.assertEquals(profileFromPurchaseHistory.getLastName(), "bernard");

    appManager.stopAll();
  }

  private ServiceManager getServiceManager(ApplicationManager appManager) throws InterruptedException {
    // Start UserProfileService
    ServiceManager userProfileServiceManager = appManager.startService(UserProfileServiceHandler.SERVICE_NAME);

    // Wait for service startup
    serviceStatusCheck(userProfileServiceManager, true);
    return userProfileServiceManager;
  }

  private void serviceStatusCheck(ServiceManager serviceManger, boolean running) throws InterruptedException {
    int trial = 0;
    while (trial++ < 5) {
      if (serviceManger.isRunning() == running) {
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    throw new IllegalStateException("Service state not executed. Expected " + running);
  }
}
