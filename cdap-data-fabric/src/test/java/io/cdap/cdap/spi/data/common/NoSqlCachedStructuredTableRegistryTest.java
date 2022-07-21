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

package io.cdap.cdap.spi.data.common;

import com.google.inject.Key;
import com.google.inject.name.Names;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import io.cdap.cdap.spi.data.nosql.NoSqlStructuredTableRegistry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.tephra.TransactionManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

/**
 * NoSQL backend for {@link CachedStructuredTableRegistryTest}
 */
public class NoSqlCachedStructuredTableRegistryTest extends CachedStructuredTableRegistryTest {
  @ClassRule
  public static final DatasetFrameworkTestUtil DS_FRAMEWORK_UTIL = new DatasetFrameworkTestUtil();

  private static TransactionManager txManager;
  private static NoSqlStructuredTableRegistry noSqlRegistry;
  private static StructuredTableRegistry registry;

  @BeforeClass
  public static void beforeClass() {
    Configuration txConf = HBaseConfiguration.create();
    txManager = new TransactionManager(txConf);
    txManager.startAndWait();

    CConfiguration cConf = DS_FRAMEWORK_UTIL.getConfiguration();
    cConf.set(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION, Constants.Dataset.DATA_STORAGE_NOSQL);

    noSqlRegistry = new NoSqlStructuredTableRegistry(DS_FRAMEWORK_UTIL.getInjector().getInstance(
      Key.get(DatasetDefinition.class, Names.named(Constants.Dataset.TABLE_TYPE_NO_TX))));
    registry = new CachedStructuredTableRegistry(noSqlRegistry);
  }

  @Override
  protected StructuredTableRegistry getStructuredTableRegistry() {
    return registry;
  }

  @Override
  protected StructuredTableRegistry getNonCachedStructuredTableRegistry() {
    return noSqlRegistry;
  }

  @AfterClass
  public static void afterClass() {
    if (txManager != null) {
      txManager.stopAndWait();
    }
  }
}
