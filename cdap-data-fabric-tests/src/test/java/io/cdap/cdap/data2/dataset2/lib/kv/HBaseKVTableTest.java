/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.kv;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.namespace.SimpleNamespaceQueryAdmin;
import co.cask.cdap.data.hbase.HBaseTestBase;
import co.cask.cdap.data.hbase.HBaseTestFactory;
import co.cask.cdap.data2.util.hbase.HBaseDDLExecutorFactory;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.spi.hbase.HBaseDDLExecutor;
import co.cask.cdap.test.SlowTests;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/**
 *
 */
@Category(SlowTests.class)
public class HBaseKVTableTest extends NoTxKeyValueTableTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  @ClassRule
  public static final HBaseTestBase TEST_HBASE = new HBaseTestFactory().get();

  private static HBaseTableUtil hBaseTableUtil = new HBaseTableUtilFactory(CConfiguration.create(),
                                                                           new SimpleNamespaceQueryAdmin()).get();

  private static HBaseDDLExecutor ddlExecutor;

  @BeforeClass
  public static void beforeClass() throws Exception {
    ddlExecutor = new HBaseDDLExecutorFactory(CConfiguration.create(),
                                              TEST_HBASE.getHBaseAdmin().getConfiguration()).get();
    ddlExecutor.createNamespaceIfNotExists(hBaseTableUtil.getHBaseNamespace(NAMESPACE_ID));
  }

  @AfterClass
  public static void afterClass() throws Exception {
    hBaseTableUtil.deleteAllInNamespace(ddlExecutor,
                                        hBaseTableUtil.getHBaseNamespace(NAMESPACE_ID),
                                        TEST_HBASE.getHBaseAdmin().getConfiguration());
    ddlExecutor.deleteNamespaceIfExists(hBaseTableUtil.getHBaseNamespace(NAMESPACE_ID));
  }

  @Override
  protected DatasetDefinition<? extends NoTxKeyValueTable, ? extends DatasetAdmin> getDefinition() {
    Injector injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        bind(CConfiguration.class).toInstance(CConfiguration.create());
        bind(Configuration.class).toInstance(TEST_HBASE.getConfiguration());
        bind(HBaseTableUtil.class).toInstance(hBaseTableUtil);
      }
    });

    HBaseKVTableDefinition def = new HBaseKVTableDefinition("foo");
    injector.injectMembers(def);
    return def;
  }
}
