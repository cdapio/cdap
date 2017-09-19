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

package co.cask.cdap.messaging.store.hbase;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.NamespaceClientUnitTestModule;
import co.cask.cdap.data.hbase.HBaseTestBase;
import co.cask.cdap.data2.util.hbase.ConfigurationReader;
import co.cask.cdap.data2.util.hbase.ConfigurationWriter;
import co.cask.cdap.data2.util.hbase.HBaseDDLExecutorFactory;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.messaging.store.MetadataTable;
import co.cask.cdap.messaging.store.PayloadTable;
import co.cask.cdap.messaging.store.PayloadTableTest;
import co.cask.cdap.messaging.store.TableFactory;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.spi.hbase.HBaseDDLExecutor;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

/**
 * HBase implementation of {@link PayloadTableTest}.
 */
public class HBasePayloadTableTestRun extends PayloadTableTest {

  @ClassRule
  public static final ExternalResource TEST_BASE = HBaseMessageTestSuite.TEST_BASE;

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final HBaseTestBase HBASE_TEST_BASE = HBaseMessageTestSuite.HBASE_TEST_BASE;
  private static final CConfiguration cConf = CConfiguration.create();

  private static Configuration hConf;
  private static HBaseTableUtil tableUtil;
  private static TableFactory tableFactory;
  private static HBaseDDLExecutor ddlExecutor;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    hConf = HBASE_TEST_BASE.getConfiguration();
    hConf.set(HBaseTableUtil.CFG_HBASE_TABLE_COMPRESSION, HBaseTableUtil.CompressionType.NONE.name());

    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.CFG_HDFS_NAMESPACE, cConf.get(Constants.CFG_LOCAL_DATA_DIR));
    cConf.set(Constants.CFG_HDFS_USER, System.getProperty("user.name"));

    tableUtil = new HBaseTableUtilFactory(cConf).get();
    ddlExecutor = new HBaseDDLExecutorFactory(cConf, hConf).get();
    ddlExecutor.createNamespaceIfNotExists(tableUtil.getHBaseNamespace(NamespaceId.SYSTEM));

    LocationFactory locationFactory = getInjector().getInstance(LocationFactory.class);
    tableFactory = new HBaseTableFactory(cConf, hConf, tableUtil, locationFactory);

    new ConfigurationWriter(hConf, cConf).write(ConfigurationReader.Type.DEFAULT, cConf);
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    tableUtil.deleteAllInNamespace(ddlExecutor, tableUtil.getHBaseNamespace(NamespaceId.SYSTEM), hConf);
    ddlExecutor.deleteNamespaceIfExists(tableUtil.getHBaseNamespace(NamespaceId.SYSTEM));
    ddlExecutor.close();
  }

  @Override
  protected PayloadTable getPayloadTable() throws Exception {
    return tableFactory.createPayloadTable(cConf.get(Constants.MessagingSystem.PAYLOAD_TABLE_NAME));
  }

  @Override
  protected MetadataTable getMetadataTable() throws Exception {
    return tableFactory.createMetadataTable(cConf.get(Constants.MessagingSystem.METADATA_TABLE_NAME));
  }

  public static Injector getInjector() {
    return Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new NamespaceClientUnitTestModule().getModule(),
      new LocationRuntimeModule().getDistributedModules());
  }
}
