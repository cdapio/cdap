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

package com.continuuity.data2.dataset.lib.table.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.hbase.HBaseTestFactory;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.BufferingOcTableClientTest;
import com.continuuity.data2.dataset.lib.table.ConflictDetection;
import com.continuuity.data2.util.hbase.HBaseTableUtil;
import com.continuuity.data2.util.hbase.HBaseTableUtilFactory;
import com.continuuity.tephra.Transaction;
import com.continuuity.tephra.TxConstants;
import com.continuuity.tephra.inmemory.DetachedTxSystemClient;
import com.continuuity.test.SlowTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.HDFSLocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 *
 */
@Category(SlowTests.class)
public class HBaseOcTableClientTest extends BufferingOcTableClientTest<HBaseOcTableClient> {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseOcTableClientTest.class);
  private static HBaseTestBase testHBase;

  @BeforeClass
  public static void beforeClass() throws Exception {
    testHBase = new HBaseTestFactory().get();
    testHBase.startHBase();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    testHBase.stopHBase();
  }

  @Override
  protected HBaseOcTableClient getTable(String name, ConflictDetection level) throws Exception {
    return new HBaseOcTableClient(name, level, testHBase.getConfiguration());
  }

  @Override
  protected DataSetManager getTableManager() throws Exception {
    Configuration hConf = testHBase.getConfiguration();
    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.CFG_HDFS_USER, System.getProperty("user.name"));
    HBaseTableUtil tableUtil = new HBaseTableUtilFactory().get();
    return new HBaseOcTableManager(conf, hConf, new HDFSLocationFactory(hConf), tableUtil);
  }

  @Test
  public void testTTL() throws Exception {
    // for the purpose of this test it is fine not to configure ttl when creating table: we want to see if it
    // applies on reading
    Properties props = new Properties();
    props.setProperty(TxConstants.PROPERTY_TTL, Integer.toString(1000));
    getTableManager().create("ttl", props);
    HBaseOcTableClient table = new HBaseOcTableClient("ttl", ConflictDetection.ROW, 1000, testHBase.getConfiguration());

    DetachedTxSystemClient txSystemClient = new DetachedTxSystemClient();
    Transaction tx = txSystemClient.startShort();
    table.startTx(tx);
    table.put(b("row1"), b("col1"), b("val1"));
    table.commitTx();

    TimeUnit.SECONDS.sleep(2);

    tx = txSystemClient.startShort();
    table.startTx(tx);
    table.put(b("row2"), b("col2"), b("val2"));
    table.commitTx();

    // now, we should not see first as it should have expired, but see the last one
    LOG.info("Checking write hiding due to TTL");
    tx = txSystemClient.startShort();
    table.startTx(tx);
    Assert.assertNull(table.get(b("row1"), b("col1")));
    Assert.assertArrayEquals(b("val2"), table.get(b("row2"), b("col2")));

    // test a table with no TTL
    Properties newProps = new Properties();
    newProps.setProperty(TxConstants.PROPERTY_TTL, Integer.toString(-1));
    getTableManager().create("nottl", newProps);
    table = new HBaseOcTableClient("nottl", ConflictDetection.ROW, -1, testHBase.getConfiguration());
    tx = txSystemClient.startShort();
    table.startTx(tx);
    table.put(b("row1"), b("col1"), b("val1"));
    table.commitTx();

    TimeUnit.SECONDS.sleep(2);

    tx = txSystemClient.startShort();
    table.startTx(tx);
    table.put(b("row2"), b("col2"), b("val2"));
    table.commitTx();

    // if ttl is -1 (unlimited), it should see both
    tx = txSystemClient.startShort();
    table.startTx(tx);
    Assert.assertArrayEquals(b("val1"), table.get(b("row1"), b("col1")));
    Assert.assertArrayEquals(b("val2"), table.get(b("row2"), b("col2")));
  }

  private static byte[] b(String s) {
    return Bytes.toBytes(s);
  }
}
