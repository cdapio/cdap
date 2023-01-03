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

package io.cdap.cdap.test.app;

import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.base.TestFrameworkTestBase;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;

/**
 * Tests ability to perform SQL query on datasets.
 */
public class TestSQLQueryTestRun extends TestFrameworkTestBase {

  private final NamespaceId testSpace = new NamespaceId("testspace");
  private static NamespaceAdmin namespaceAdmin;

  @BeforeClass
  public static void init() throws Exception {
    namespaceAdmin = getNamespaceAdmin();
  }

  @Test(timeout = 90000L)
  public void testSQLQuerySimpleNS() throws Exception {
    namespaceAdmin.create(new NamespaceMeta.Builder().setName(testSpace).build());
    testSQLQuery();
    namespaceAdmin.delete(testSpace);
  }

  private void checkDatabaseExists(String customHiveDatabase) throws Exception {
    try (
      // list all databases in hive
      Connection connection = getQueryClient();
      ResultSet results = connection.prepareStatement("SHOW DATABASES")
        .executeQuery()
    ) {
      Assert.assertNotNull(results);
      Assert.assertTrue(results.next());
      // verify that the hive databases has the given custom database
      Assert.assertEquals(customHiveDatabase, results.getString(1));
      Assert.assertTrue(results.next());
      // default is always expected to exists
      Assert.assertEquals("default", results.getString(1));
      Assert.assertFalse(results.next());
    }
  }

  private void testSQLQuery() throws Exception {
    // Deploying app makes sure that the default namespace is available.
    deployApplication(testSpace, DummyApp.class);
    deployDatasetModule(testSpace.datasetModule("my-kv"), AppsWithDataset.KeyValueTableDefinition.Module.class);
    deployApplication(testSpace, AppsWithDataset.AppWithAutoCreate.class);
    DataSetManager<AppsWithDataset.KeyValueTableDefinition.KeyValueTable> myTableManager =
      getDataset(testSpace.dataset("myTable"));
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
  }
}
