/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package io.cdap.cdap.data2.util.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.data.hbase.HBaseTestBase;
import io.cdap.cdap.data.hbase.HBaseTestFactory;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.hbase.HBaseDDLExecutor;
import io.cdap.cdap.test.SlowTests;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests reading and writing {@link CConfiguration} instances to an HBase table.
 */
@Category(SlowTests.class)
public class ConfigurationTableTest {

  @ClassRule
  public static final HBaseTestBase TEST_HBASE = new HBaseTestFactory().get();

  private static HBaseTableUtil tableUtil;
  private static CConfiguration cConf = CConfiguration.create();
  private static HBaseDDLExecutor ddlExecutor;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    tableUtil = new HBaseTableUtilFactory(cConf).get();
    ddlExecutor = new HBaseDDLExecutorFactory(cConf, TEST_HBASE.getHBaseAdmin().getConfiguration()).get();
    ddlExecutor.createNamespaceIfNotExists(tableUtil.getHBaseNamespace(NamespaceId.SYSTEM));
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    tableUtil.deleteAllInNamespace(ddlExecutor, tableUtil.getHBaseNamespace(NamespaceId.SYSTEM),
                                   TEST_HBASE.getHBaseAdmin().getConfiguration());
    ddlExecutor.deleteNamespaceIfExists(tableUtil.getHBaseNamespace(NamespaceId.SYSTEM));
  }

  @Test
  public void testConfigurationSerialization() throws Exception {
    Configuration hConf = TEST_HBASE.getConfiguration();
    ConfigurationWriter writer = new ConfigurationWriter(hConf, cConf);
    ConfigurationReader reader = new ConfigurationReader(hConf, cConf);

    // read should yield null if table does not exist
    CConfiguration cConfBeforeTableExists = reader.read(ConfigurationReader.Type.DEFAULT);
    Assert.assertNull(cConfBeforeTableExists);

    // read should yield null if table does exists but no configuration has been written yet
    writer.createTableIfNecessary();
    CConfiguration cConfBeforeConfigWritten = reader.read(ConfigurationReader.Type.DEFAULT);
    Assert.assertNull(cConfBeforeConfigWritten);

    // now write a cConf with an additional property
    CConfiguration cConfWithAddedProperty = CConfiguration.copy(cConf);
    cConfWithAddedProperty.set("additional.property", "additional.property.value");
    writer.write(ConfigurationReader.Type.DEFAULT, cConfWithAddedProperty);
    CConfiguration cConfAfterWrite = reader.read(ConfigurationReader.Type.DEFAULT);
    assertNotNull(cConfAfterWrite);
    validateCConf(cConfWithAddedProperty, cConfAfterWrite);

    // now write without the additional value and read back
    writer.write(ConfigurationReader.Type.DEFAULT, cConf);
    CConfiguration cConfAfter2ndWrite = reader.read(ConfigurationReader.Type.DEFAULT);
    assertNotNull(cConfAfter2ndWrite);
    validateCConf(cConf, cConfAfter2ndWrite);
  }

  private void validateCConf(CConfiguration expected, CConfiguration actual) {
    for (Map.Entry<String, String> e : expected) {
      assertEquals("Configuration value mismatch (expected -> actual) for key: " + e.getKey(),
                   e.getValue(), actual.get(e.getKey()));
    }
    for (Map.Entry<String, String> e : actual) {
      assertEquals("Configuration value mismatch (actual -> expected) for key: " + e.getKey(),
                   e.getValue(), expected.get(e.getKey()));
    }
  }
}
