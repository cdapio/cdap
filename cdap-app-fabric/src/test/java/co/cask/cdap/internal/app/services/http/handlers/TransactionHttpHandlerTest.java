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
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for Transaction handlers.
 */
public class TransactionHttpHandlerTest extends AppFabricTestBase {

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
}
