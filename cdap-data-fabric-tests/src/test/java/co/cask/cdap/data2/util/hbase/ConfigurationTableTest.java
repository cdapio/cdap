/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.data2.util.hbase;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.hbase.HBaseTestBase;
import co.cask.cdap.data.hbase.HBaseTestFactory;
import co.cask.cdap.test.SlowTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests reading and writing {@link CConfiguration} instances to an HBase table.
 */
@Category(SlowTests.class)
public class ConfigurationTableTest {
  private static HBaseTableUtil tableUtil = new HBaseTableUtilFactory().get();
  private static HBaseTestBase testHBase = new HBaseTestFactory().get();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    testHBase.startHBase();
    tableUtil.createNamespaceIfNotExists(testHBase.getHBaseAdmin(), Constants.SYSTEM_NAMESPACE_ID);
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    testHBase.deleteTables(HTableNameConverter.toHBaseNamespace(Constants.SYSTEM_NAMESPACE_ID));
    tableUtil.deleteNamespaceIfExists(testHBase.getHBaseAdmin(), Constants.SYSTEM_NAMESPACE_ID);
    testHBase.stopHBase();
  }

  @Test
  public void testConfigurationSerialization() throws Exception {
    CConfiguration cconf = CConfiguration.create();
    String expectedNamespace = cconf.get(Constants.Dataset.TABLE_PREFIX);

    ConfigurationTable configTable = new ConfigurationTable(testHBase.getConfiguration());
    configTable.write(ConfigurationTable.Type.DEFAULT, cconf);

    String configTableQualifier = "configuration";
    TableId configTableId = TableId.from(String.format("%s.system.%s", expectedNamespace, configTableQualifier));
    String configTableName = tableUtil.createHTableDescriptor(configTableId).getNameAsString();
    // the config table name minus the qualifier ('configuration'). Example: 'cdap.system.'
    String configTablePrefix = configTableName.substring(0, configTableName.length()  - configTableQualifier.length());

    CConfiguration cconf2 = configTable.read(ConfigurationTable.Type.DEFAULT, configTablePrefix);
    assertNotNull(cconf2);

    for (Map.Entry<String, String> e : cconf) {
      assertEquals("Configuration value mismatch (cconf -> cconf2) for key: " + e.getKey(),
                   e.getValue(), cconf2.get(e.getKey()));
    }
    for (Map.Entry<String, String> e : cconf2) {
      assertEquals("Configuration value mismatch (cconf2 -> cconf) for key: " + e.getKey(),
                   e.getValue(), cconf.get(e.getKey()));
    }
  }
}
