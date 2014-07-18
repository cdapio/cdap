/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.data2.transaction.hbase;

import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;

/**
 * Common test class for TransactionVisibilityFilter implementations.
 */
public abstract class AbstractTransactionVisibilityFilterTest {

  protected static final byte[] FAM = Bytes.toBytes("f");
  protected static final byte[] FAM2 = Bytes.toBytes("f2");
  protected static final byte[] FAM3 = Bytes.toBytes("f3");
  protected static final byte[] COL = Bytes.toBytes("c");
  protected static final List<byte[]> EMPTY_CHANGESET = Lists.newArrayListWithCapacity(0);

  protected InMemoryTransactionManager txManager;

  @Before
  public void setup() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.unset(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES);
    txManager = new InMemoryTransactionManager(conf);
    txManager.startAndWait();
  }

  @After
  public void tearDown() throws Exception {
    txManager.stopAndWait();
  }

  /**
   * Creates a new TransactionVisibilityFilter for the specific HBase version of the implementation.
   */
  protected abstract Filter createFilter(Transaction tx, Map<byte[], Long> familyTTLs);
}
