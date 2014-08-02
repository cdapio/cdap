/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package co.cask.cdap.data;

import co.cask.cdap.data.hbase.HBaseTestBase;
import co.cask.cdap.data.hbase.HBaseTestFactory;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.test.SlowTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.HDFSLocationFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

/**
 *
 */
@Category(SlowTests.class)
public class DistributedDataSetAccessorTest extends NamespacingDataSetAccessorTest {
  private static DataSetAccessor dsAccessor;

  private static HBaseTestBase testHBase;

  @BeforeClass
  public static void beforeClass() throws Exception {
    NamespacingDataSetAccessorTest.beforeClass();
    testHBase = new HBaseTestFactory().get();
    testHBase.startHBase();
    Configuration hConf = testHBase.getConfiguration();
    HBaseTableUtil tableUtil = new HBaseTableUtilFactory().get();
    dsAccessor = new DistributedDataSetAccessor(conf, hConf, new HDFSLocationFactory(hConf), tableUtil);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    testHBase.stopHBase();
  }

  @Override
  protected DataSetAccessor getDataSetAccessor() {
    return dsAccessor;
  }
}
