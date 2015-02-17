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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.data2.dataset2.AbstractDatasetTest;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionExecutor;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

/**
 * Tests dataset instantiation with arguments.
 */
public class DatasetWithArgumentsTest extends AbstractDatasetTest {

  private static final Id.DatasetModule prefix = Id.DatasetModule.from(NAMESPACE_ID, "prefix");
  private static final Id.DatasetInstance pret = Id.DatasetInstance.from(NAMESPACE_ID, "pret");

  @BeforeClass
  public static void beforeClass() throws Exception {
    addModule(prefix, new PrefixedTableModule());
    createInstance("prefixedTable", pret, DatasetProperties.EMPTY);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    deleteInstance(pret);
    deleteModule(prefix);
  }

  @Test
  public void testPrefixTable() throws Exception {
    final PrefixedTable table = getInstance(pret, null);
    final PrefixedTable aTable = getInstance(pret, Collections.singletonMap("prefix", "a"));
    final PrefixedTable bTable = getInstance(pret, Collections.singletonMap("prefix", "b"));

    TransactionExecutor txnl = newTransactionExecutor(aTable, bTable, table);

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // write some values
        table.write("z", "0");
        aTable.write("x", "1");
        bTable.write("x", "2");
        bTable.write("y", "3");
      }
    });

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // read all values without prefix
        Assert.assertEquals("0", table.read("z"));
        Assert.assertEquals("1", table.read("ax"));
        Assert.assertEquals("2", table.read("bx"));
        Assert.assertEquals("3", table.read("by"));

        // read all values with prefix a
        Assert.assertEquals("1", aTable.read("x"));
        Assert.assertNull(aTable.read("y"));
        Assert.assertNull(aTable.read("z"));

        // read all values with prefix b
        Assert.assertEquals("2", bTable.read("x"));
        Assert.assertEquals("3", bTable.read("y"));
        Assert.assertNull(aTable.read("z"));
      }
    });
  }

}
