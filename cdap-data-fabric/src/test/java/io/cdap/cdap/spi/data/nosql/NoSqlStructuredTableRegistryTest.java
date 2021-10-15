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

import com.google.inject.Key;
import com.google.inject.name.Names;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import io.cdap.cdap.spi.data.common.StructuredTableRegistry;
import io.cdap.cdap.spi.data.common.StructuredTableRegistryTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.tephra.TransactionManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

/**
 *
 */
public class NoSqlStructuredTableRegistryTest extends StructuredTableRegistryTest {
  @ClassRule
  public static final DatasetFrameworkTestUtil DS_FRAMEWORK_UTIL = new DatasetFrameworkTestUtil();

  private static TransactionManager txManager;

  @Override
  protected StructuredTableRegistry getStructuredTableRegistry() {
    return new NoSqlStructuredTableRegistry(DS_FRAMEWORK_UTIL.getInjector().getInstance(
      Key.get(DatasetDefinition.class, Names.named(Constants.Dataset.TABLE_TYPE_NO_TX))));
  }

  @BeforeClass
  public static void beforeClass() {
    Configuration txConf = HBaseConfiguration.create();
    txManager = new TransactionManager(txConf);
    txManager.startAndWait();
  }

  @AfterClass
  public static void afterClass() {
    if (txManager != null) {
      txManager.stopAndWait();
    }
  }
}
