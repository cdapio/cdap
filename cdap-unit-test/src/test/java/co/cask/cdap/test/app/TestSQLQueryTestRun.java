/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.test.app;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;

/**
 * Tests ability to perform SQL query on datasets.
 */
public class TestSQLQueryTestRun extends TestFrameworkTestBase {

  private final Id.Namespace testSpace = Id.Namespace.from("testspace");

  @Before
  public void setUp() throws Exception {
    // create the namespace with a custom hive database name
    getNamespaceAdmin().create(new NamespaceMeta.Builder().setName(testSpace).setHiveDatabase("custom_db").build());
  }

  @Test(timeout = 90000L)
  public void testSQLQuery() throws Exception {
    // Deploying app makes sure that the default namespace is available.
    deployApplication(testSpace, DummyApp.class);
    deployDatasetModule(testSpace, "my-kv", AppsWithDataset.KeyValueTableDefinition.Module.class);
    deployApplication(testSpace, AppsWithDataset.AppWithAutoCreate.class);
    DataSetManager<AppsWithDataset.KeyValueTableDefinition.KeyValueTable> myTableManager =
      getDataset(testSpace, "myTable");
    AppsWithDataset.KeyValueTableDefinition.KeyValueTable kvTable = myTableManager.get();
    kvTable.put("a", "1");
    kvTable.put("b", "2");
    kvTable.put("c", "1");
    myTableManager.flush();

    try (
      Connection connection = getQueryClient(testSpace);
      ResultSet results = connection.prepareStatement("select first from dataset_mytable where second = '1'")
        .executeQuery()
    ) {
      // run a query over the dataset
      Assert.assertTrue(results.next());
      Assert.assertEquals("a", results.getString(1));
      Assert.assertTrue(results.next());
      Assert.assertEquals("c", results.getString(1));
      Assert.assertFalse(results.next());
    }

    try (
      // list all databases in hive
      Connection connection = getQueryClient(testSpace);
      ResultSet results = connection.prepareStatement("SHOW DATABASES")
        .executeQuery()
    ) {
      Assert.assertNotNull(results);
      Assert.assertTrue(results.next());
      // verify that the hive databases has the custom database given for the namespace
      Assert.assertEquals("custom_db", results.getString(1));
      Assert.assertTrue(results.next());
      Assert.assertEquals("default", results.getString(1));
      Assert.assertFalse(results.next());
    }
  }
}
