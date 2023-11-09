/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.store;

import com.google.inject.Injector;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests {@link AppMetadataStore} using NoSQL.
 */
public class NoSqlAppMetadataStoreTest extends AppMetadataStoreTest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    Injector injector = AppFabricTestHelper.getInjector();
    AppFabricTestHelper.ensureNamespaceExists(NamespaceId.DEFAULT);
    transactionRunner = injector.getInstance(TransactionRunner.class);
  }

  @AfterClass
  public static void tearDown() {
    AppFabricTestHelper.shutdown();
  }

  @Override @Test(expected = UnsupportedOperationException.class)
  public void testScanApplicationsReverse() {
    super.testScanApplicationsReverse();
  }

  @Override @Test(expected = UnsupportedOperationException.class)
  public void testScanApplicationsWithNamespaceReverse() {
    super.testScanApplicationsWithNamespaceReverse();
  }

  /**
   * This testcase tries to create a row in the app spec table, with the latest column set
   * to null (To test the getLatest method for applications deployed before v6.8.0). Setting
   * a Boolean column to null in the NoSqlStructuredTable is expected to throw a NullPointerException.
   */
  @Override @Test(expected = NullPointerException.class)
  public void testGetLatestOnLegacyRows() throws Exception {
    super.testGetLatestOnLegacyRows();
  }
}
