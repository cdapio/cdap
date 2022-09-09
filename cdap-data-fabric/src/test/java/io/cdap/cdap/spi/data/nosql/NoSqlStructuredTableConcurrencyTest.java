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

package io.cdap.cdap.spi.data.nosql;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.StructuredTableConcurrencyTest;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;

/**
 * Tests concurrent operations on {@link NoSqlStructuredTable}.
 */
public class NoSqlStructuredTableConcurrencyTest extends StructuredTableConcurrencyTest {
  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  private static TransactionManager txManager;
  private static StructuredTableAdmin noSqlTableAdmin;
  private static TransactionRunner transactionRunner;

  @Override
  protected StructuredTableAdmin getStructuredTableAdmin() {
    return noSqlTableAdmin;
  }

  @Override
  protected TransactionRunner getTransactionRunner() {
    return transactionRunner;
  }

  @BeforeClass
  public static void beforeClass() throws IOException {
    Configuration txConf = new Configuration();
    txManager = new TransactionManager(txConf);
    txManager.startAndWait();

    CConfiguration cConf = dsFrameworkUtil.getConfiguration();
    cConf.set(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION, Constants.Dataset.DATA_STORAGE_NOSQL);
    noSqlTableAdmin = dsFrameworkUtil.getInjector().getInstance(StructuredTableAdmin.class);
    transactionRunner = dsFrameworkUtil.getInjector().getInstance(TransactionRunner.class);
  }

  @AfterClass
  public static void afterClass() {
    if (txManager != null) {
      txManager.stopAndWait();
    }
  }
}
