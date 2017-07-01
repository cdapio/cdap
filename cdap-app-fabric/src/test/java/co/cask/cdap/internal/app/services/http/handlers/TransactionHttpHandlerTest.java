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

package co.cask.cdap.internal.app.services.http.handlers;

import co.cask.cdap.app.test.AppFabricDatasetTester;
import co.cask.cdap.gateway.handlers.TransactionHttpHandler;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionSystemClient;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests for Transaction handlers.
 */
public class TransactionHttpHandlerTest {

  @ClassRule
  public static final AppFabricDatasetTester TESTER = new AppFabricDatasetTester(TransactionHttpHandler.class);

  private static final Gson GSON = new Gson();
  private static final Type STRING_INT_TYPE = new TypeToken<Map<String, Integer>>() { }.getType();

  /**
   * Tests invalidating a transaction.
   * @throws Exception
   */
  @Test
  public void testInvalidateTx() throws Exception {
    TransactionSystemClient txClient = TESTER.getTxClient();

    Transaction tx1 = txClient.startShort();
    HttpResponse response = HttpRequests.execute(
      HttpRequest.post(TESTER.getEndpointURL("/v3/transactions/" + tx1.getWritePointer() + "/invalidate")).build());

    Assert.assertEquals(200, response.getResponseCode());

    Transaction tx2 = txClient.startShort();
    txClient.commit(tx2);
    response = HttpRequests.execute(
      HttpRequest.post(TESTER.getEndpointURL("/v3/transactions/" + tx1.getWritePointer() + "/invalidate")).build());
    Assert.assertEquals(409, response.getResponseCode());

    response = HttpRequests.execute(
      HttpRequest.post(TESTER.getEndpointURL("/v3/transactions/foobar/invalidate")).build());

    Assert.assertEquals(400, response.getResponseCode());
  }

  @Test
  public void testResetTxManagerState() throws Exception {
    HttpResponse response = HttpRequests.execute(
      HttpRequest.post(TESTER.getEndpointURL("/v3/transactions/state")).build());

    Assert.assertEquals(200, response.getResponseCode());
  }

  @Test
  public void testTruncateInvalidTx() throws Exception {
    TransactionSystemClient txClient = TESTER.getTxClient();
    // Reset state, and assert no invalid transactions are present
    txClient.resetState();
    Assert.assertEquals(0, txClient.getInvalidSize());

    // Start few transactions and invalidate them
    Transaction tx1 = txClient.startShort();
    Transaction tx2 = txClient.startLong();
    Transaction tx3 = txClient.startLong();

    Assert.assertTrue(txClient.invalidate(tx1.getWritePointer()));
    Assert.assertTrue(txClient.invalidate(tx2.getWritePointer()));
    Assert.assertTrue(txClient.invalidate(tx3.getWritePointer()));

    Assert.assertEquals(3, txClient.getInvalidSize());

    // Remove tx1 and tx3 from invalid list
    HttpResponse response = HttpRequests.execute(
      HttpRequest
        .post(TESTER.getEndpointURL("/v3/transactions/invalid/remove/ids"))
        .withBody(GSON.toJson(Collections.singletonMap("ids", Arrays.asList(tx1.getWritePointer(),
                                                                            tx3.getWritePointer()))))
        .build());

    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals(1, txClient.getInvalidSize());
  }

  @Test
  public void testTruncateInvalidTxBefore() throws Exception {
    TransactionSystemClient txClient = TESTER.getTxClient();
    // Reset state, and assert no invalid transactions are present
    txClient.resetState();
    Assert.assertEquals(0, txClient.getInvalidSize());

    // Start few transactions and invalidate them
    Transaction tx1 = txClient.startShort();
    Transaction tx2 = txClient.startLong();
    // Sleep so that transaction ids get generated a millisecond apart for assertion
    // TEPHRA-63 should eliminate the need to sleep
    TimeUnit.MILLISECONDS.sleep(1);
    long beforeTx3 = System.currentTimeMillis();
    Transaction tx3 = txClient.startLong();

    Assert.assertTrue(txClient.invalidate(tx1.getWritePointer()));
    Assert.assertTrue(txClient.invalidate(tx2.getWritePointer()));
    Assert.assertTrue(txClient.invalidate(tx3.getWritePointer()));

    Assert.assertEquals(3, txClient.getInvalidSize());

    // Remove all transactions in invalid list beforeTx3
    HttpResponse response = HttpRequests.execute(
      HttpRequest
        .post(TESTER.getEndpointURL("/v3/transactions/invalid/remove/until"))
        .withBody(GSON.toJson(Collections.singletonMap("time", beforeTx3)))
        .build());

    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals(1, txClient.getInvalidSize());
  }

  @Test
  public void testGetInvalidSize() throws Exception {
    TransactionSystemClient txClient = TESTER.getTxClient();
    // Reset state, and assert no invalid transactions are present
    txClient.resetState();
    Assert.assertEquals(0, txClient.getInvalidSize());

    // Start few transactions and invalidate them
    Transaction tx1 = txClient.startShort();
    Transaction tx2 = txClient.startLong();
    Transaction tx3 = txClient.startLong();

    Assert.assertTrue(txClient.invalidate(tx1.getWritePointer()));
    Assert.assertTrue(txClient.invalidate(tx2.getWritePointer()));
    Assert.assertTrue(txClient.invalidate(tx3.getWritePointer()));

    Assert.assertEquals(3, txClient.getInvalidSize());

    // Assert through REST API
    HttpResponse response = HttpRequests.execute(
      HttpRequest.get(TESTER.getEndpointURL("/v3/transactions/invalid/size")).build());

    Assert.assertEquals(200, response.getResponseCode());
    Map<String, Integer> resultMap = GSON.fromJson(response.getResponseBodyAsString(), STRING_INT_TYPE);
    Assert.assertNotNull(resultMap);
    Assert.assertEquals(3, (int) resultMap.get("size"));
  }
}
