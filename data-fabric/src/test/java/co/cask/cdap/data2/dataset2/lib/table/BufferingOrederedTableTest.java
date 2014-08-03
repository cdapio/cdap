/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.table.OrderedTable;
import co.cask.cdap.data2.dataset.lib.table.BufferingOcTableClient;
import co.cask.cdap.data2.dataset.lib.table.BufferingOcTableClientTest;
import com.continuuity.tephra.Transaction;
import com.continuuity.tephra.TransactionAware;
import org.junit.Assert;
import org.junit.Test;

/**
 * unit-test
 * @param <T> table type
 */
public abstract class BufferingOrederedTableTest<T extends BufferingOcTableClient>
  extends OrderedTableConcurrentTest<T> {

  @Test
  public void testRollingBackAfterExceptionDuringPersist() throws Exception {
    DatasetAdmin admin = getTableAdmin("myTable");
    admin.create();
    try {
      Transaction tx1 = txClient.startShort();
      BufferingOcTableClient myTable1 =
        new BufferingOcTableClientTest.BufferingOcTableWithPersistingFailure(getTable("myTable"));
      myTable1.startTx(tx1);
      // write some data but not commit
      myTable1.put(R1, a(C1), a(V1));
      myTable1.put(R2, a(C2), a(V2));
      // verify can see changes inside tx
      verify(a(C1, V1), myTable1.get(R1, a(C1)));
      verify(a(C2, V2), myTable1.get(R2, a(C2)));

      // persisting changes
      try {
        // should simulate exception
        myTable1.commitTx();
        Assert.assertFalse(true);
      } catch (Throwable th) {
        // Expected simulated exception
      }

      // let's pretend that after persisting changes we still got conflict when finalizing tx, so
      // rolling back changes
      Assert.assertTrue(myTable1.rollbackTx());

      // making tx visible
      txClient.abort(tx1);

      // start new tx
      Transaction tx2 = txClient.startShort();
      OrderedTable myTable2 = getTable("myTable");
      ((TransactionAware) myTable2).startTx(tx2);

      // verify don't see rolled back changes
      verify(a(), myTable2.get(R1, a(C1)));
      verify(a(), myTable2.get(R2, a(C2)));

    } finally {
      admin.drop();
    }
  }
}
