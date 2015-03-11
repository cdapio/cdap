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

import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests for Transaction handlers.
 */
public class TransactionHttpHandlerTest extends AppFabricTestBase {

  private static final Type STRING_INT_TYPE = new TypeToken<Map<String, Integer>>() { }.getType();

  /**
   * Tests invalidating a transaction.
   * @throws Exception
   */
  @Test
  public void testInvalidateTx() throws Exception {
    TransactionSystemClient txClient = getTxClient();

    Transaction tx1 = txClient.startShort();
    HttpResponse response = doPost("/v3/transactions/" + tx1.getWritePointer() + "/invalidate");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Transaction tx2 = txClient.startShort();
    txClient.commit(tx2);
    response = doPost("/v3/transactions/" + tx2.getWritePointer() + "/invalidate");
    Assert.assertEquals(409, response.getStatusLine().getStatusCode());

    Assert.assertEquals(400,
                        doPost("/v3/transactions/foobar/invalidate").getStatusLine().getStatusCode());
  }

  @Test
  public void testResetTxManagerState() throws Exception {
    HttpResponse response = doPost("/v3/transactions/state");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }
  
  @Test
  public void testTruncateInvalidTx() throws Exception {
    TransactionSystemClient txClient = getTxClient();
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
    HttpResponse response = 
      doPost("/v3/transactions/invalid/remove/ids",
             GSON.toJson(ImmutableMap.of("ids", ImmutableSet.of(tx1.getWritePointer(), tx3.getWritePointer()))));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    
    Assert.assertEquals(1, txClient.getInvalidSize());
  }

  @Test
  public void testTruncateInvalidTxBefore() throws Exception {
    TransactionSystemClient txClient = getTxClient();
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
    HttpResponse response =
      doPost("/v3/transactions/invalid/remove/until",
             GSON.toJson(ImmutableMap.of("time", beforeTx3)));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Assert.assertEquals(1, txClient.getInvalidSize());
  }

  @Test
  public void testGetInvalidSize() throws Exception {
    TransactionSystemClient txClient = getTxClient();
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
    HttpResponse response = doGet("/v3/transactions/invalid/size");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Map<String, Integer> resultMap = GSON.fromJson(EntityUtils.toString(response.getEntity()), STRING_INT_TYPE);
    Assert.assertNotNull(resultMap);
    Assert.assertEquals(3, (int) resultMap.get("size"));
  }
}
