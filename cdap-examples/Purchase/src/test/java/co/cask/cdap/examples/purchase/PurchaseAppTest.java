/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link PurchaseApp}.
 */
public class PurchaseAppTest extends TestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  private static final Gson GSON = new Gson();

  @Test
  public void test() throws Exception {
    // Deploy the PurchaseApp application
    ApplicationManager appManager = deployApplication(PurchaseApp.class);

    // Start PurchaseFlow
    FlowManager flowManager = appManager.getFlowManager("PurchaseFlow").start();

    // Send stream events to the "purchaseStream" Stream
    StreamManager streamManager = getStreamManager("purchaseStream");
    streamManager.send("bob bought 3 apples for $30");
    streamManager.send("joe bought 1 apple for $100");
    streamManager.send("joe bought 10 pineapples for $20");
    streamManager.send("cat bought 3 bottles for $12");
    streamManager.send("cat bought 2 pops for $14");

    try {
      // Wait for the last Flowlet processing 5 events, or at most 5 seconds
      RuntimeMetrics metrics = flowManager.getFlowletMetrics("collector");
      metrics.waitForProcessed(5, 15, TimeUnit.SECONDS);
    } finally {
      flowManager.stop();
    }

    // write a few purchase records into the 'oldPurchases' dataset, which stores it in a different format
    DataSetManager<KeyValueTable> oldPurchasesManager = getDataset("oldPurchases");
    KeyValueTable oldPurchases = oldPurchasesManager.get();
    long now = System.currentTimeMillis();
    List<Purchase> purchases = ImmutableList.of(new Purchase("bob", "pineapples", 1, 4, now),
                                                new Purchase("bob", "drupes", 16, 30, now + 1),
                                                new Purchase("joe", "bananas", 4, 5, now + 2),
                                                new Purchase("joe", "oranges", 5, 3, now + 3),
                                                new Purchase("joe", "pomegranates", 1, 10, now + 4));
    for (Purchase purchase : purchases) {
      oldPurchases.write(Bytes.toBytes(purchase.getPurchaseTime()), Bytes.toBytes(GSON.toJson(purchase)));
    }
    oldPurchasesManager.flush();


    ServiceManager userProfileServiceManager = getUserProfileServiceManager(appManager);

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
    MapReduceManager mapReduceManager =
      appManager.getMapReduceManager(PurchaseHistoryBuilder.class.getSimpleName()).start();
    mapReduceManager.waitForFinish(3, TimeUnit.MINUTES);

    // Start PurchaseHistoryService
    ServiceManager purchaseHistoryServiceManager =
      appManager.getServiceManager(PurchaseHistoryService.SERVICE_NAME).start();

    // Wait for service startup
    purchaseHistoryServiceManager.waitForStatus(true);

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
    Assert.assertEquals(5, history.getPurchases().size());

    UserProfile profileFromPurchaseHistory = history.getUserProfile();
    Assert.assertEquals(profileFromPurchaseHistory.getFirstName(), "joe");
    Assert.assertEquals(profileFromPurchaseHistory.getLastName(), "bernard");
  }

  private ServiceManager getUserProfileServiceManager(ApplicationManager appManager) throws InterruptedException {
    // Start UserProfileService
    ServiceManager userProfileServiceManager =
      appManager.getServiceManager(UserProfileServiceHandler.SERVICE_NAME).start();

    // Wait for service startup
    userProfileServiceManager.waitForStatus(true);
    return userProfileServiceManager;
  }
}
