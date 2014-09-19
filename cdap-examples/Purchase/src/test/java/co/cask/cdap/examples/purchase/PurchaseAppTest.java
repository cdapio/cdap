/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.test.ProcedureManager;
import co.cask.cdap.test.RuntimeMetrics;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
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

    // Run PurchaseHistoryWorkflow which will process the data
    MapReduceManager mapReduceManager = appManager.startMapReduce("PurchaseHistoryWorkflow_PurchaseHistoryBuilder",
                                                                  ImmutableMap.<String, String>of());
    mapReduceManager.waitForFinish(3, TimeUnit.MINUTES);

    // Start PurchaseProcedure and query
    ProcedureManager procedureManager = appManager.startProcedure("PurchaseProcedure");
    String response = procedureManager.getClient().query("history", ImmutableMap.of("customer", "joe"));
    PurchaseHistory historyResponse = GSON.fromJson(response, PurchaseHistory.class);

    Assert.assertEquals("joe", historyResponse.getCustomer());
    Assert.assertEquals(2, historyResponse.getPurchases().size());
    appManager.stopAll();
  }

}
