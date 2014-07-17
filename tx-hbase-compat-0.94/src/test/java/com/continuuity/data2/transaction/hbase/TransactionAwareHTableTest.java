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

import com.continuuity.data2.transaction.TransactionContext;
import com.continuuity.data2.transaction.inmemory.DetachedTxSystemClient;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

/**
 * A Transaction Aware HTable implementation for HBase 0.94. Operations are committed as usual,
 * but upon a failed or aborted transaction, they are rolled back to the state before the transaction
 * was started.
 */
public class TransactionAwareHTableTest {
  private static HBaseTestingUtility testUtil;
  private static HBaseAdmin hBaseAdmin;
  private TransactionContext transactionContext;
  private TransactionAwareHTable transactionAwareHTable;

  private static final class TestBytes {
    private static final byte[] table = Bytes.toBytes("testtable");
    private static final byte[] family = Bytes.toBytes("testfamily");
    private static final byte[] qualifier = Bytes.toBytes("testqualifier");
    private static final byte[] row = Bytes.toBytes("testrow");
    private static final byte[] value = Bytes.toBytes("testvalue");
  }

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    testUtil = new HBaseTestingUtility();
    testUtil.startMiniCluster();
    hBaseAdmin = testUtil.getHBaseAdmin();
  }

  @AfterClass
  public static void shutdownAfterClass() throws Exception {
    testUtil.shutdownMiniCluster();
    hBaseAdmin.close();
  }

  @Before
  public void setupBeforeTest() throws IOException {
    HTable hTable = testUtil.createTable(TestBytes.table, TestBytes.family);
    transactionAwareHTable = new TransactionAwareHTable(hTable);
    transactionContext = new TransactionContext(new DetachedTxSystemClient(), transactionAwareHTable);
  }

  @After
  public void shutdownAfterTest() throws IOException {
    hBaseAdmin.disableTable(TestBytes.table);
    hBaseAdmin.deleteTable(TestBytes.table);
  }

  /**
   * Test transactional put and get requests.
   * @throws Exception
   */
  @Test
  public void testValidTransactionalPutAndGet() throws Exception {
    transactionContext.start();
    Put put = new Put(TestBytes.row);
    put.add(TestBytes.family, TestBytes.qualifier, TestBytes.value);
    transactionAwareHTable.put(put);
    transactionContext.finish();

    transactionContext.start();
    Result result = transactionAwareHTable.get(new Get(TestBytes.row));
    transactionContext.finish();

    byte[] value = result.getValue(TestBytes.family, TestBytes.qualifier);
    Assert.assertArrayEquals(TestBytes.value, value);
  }

  /**
   * Test aborted put requests, that must be rolled back.
   * @throws Exception
   */
  @Test
  public void testAbortedTransactionPutAndGet() throws Exception {
    transactionContext.start();
    Put put = new Put(TestBytes.row);
    put.add(TestBytes.family, TestBytes.qualifier, TestBytes.value);
    transactionAwareHTable.put(put);

    transactionContext.abort();

    transactionContext.start();
    Result result = transactionAwareHTable.get(new Get(TestBytes.row));
    transactionContext.finish();
    byte[] value = result.getValue(TestBytes.family, TestBytes.qualifier);
    Assert.assertArrayEquals(value, null);
  }

  /**
   * Test transactional delete operations.
   * @throws Exception
   */
  @Test
  public void testValidTransactionalDelete() throws Exception {
    transactionContext.start();
    Put put = new Put(TestBytes.row);
    put.add(TestBytes.family, TestBytes.qualifier, TestBytes.value);
    transactionAwareHTable.put(put);
    transactionContext.finish();

    transactionContext.start();
    Result result = transactionAwareHTable.get(new Get(TestBytes.row));
    transactionContext.finish();
    byte[] value = result.getValue(TestBytes.family, TestBytes.qualifier);
    Assert.assertArrayEquals(TestBytes.value, value);

    transactionContext.start();
    Delete delete = new Delete(TestBytes.row);
    transactionAwareHTable.delete(delete);

    transactionContext.finish();

    transactionContext.start();
    result = transactionAwareHTable.get(new Get(TestBytes.row));
    transactionContext.finish();
    value = result.getValue(TestBytes.family, TestBytes.qualifier);
    Assert.assertArrayEquals(value, new byte[0]);
  }

  /**
   * Test aborted transactional delete requests, that must be rolled back.
   * @throws Exception
   */
  @Test
  public void testAbortedTransactionalDelete() throws Exception {
    transactionContext.start();
    Put put = new Put(TestBytes.row);
    put.add(TestBytes.family, TestBytes.qualifier, TestBytes.value);
    transactionAwareHTable.put(put);
    transactionContext.finish();

    transactionContext.start();
    Result result = transactionAwareHTable.get(new Get(TestBytes.row));
    transactionContext.finish();
    byte[] value = result.getValue(TestBytes.family, TestBytes.qualifier);
    Assert.assertArrayEquals(TestBytes.value, value);

    transactionContext.start();
    Delete delete = new Delete(TestBytes.row);
    transactionAwareHTable.delete(delete);
    transactionContext.abort();

    transactionContext.start();
    result = transactionAwareHTable.get(new Get(TestBytes.row));
    transactionContext.finish();
    value = result.getValue(TestBytes.family, TestBytes.qualifier);
    Assert.assertArrayEquals(TestBytes.value, value);
  }

  /**
   * Expect an exception since a transaction hasn't been started.
   * @throws Exception
   */
  @Test(expected = IOException.class)
  public void testTransactionlessFailure() throws Exception {
    transactionAwareHTable.get(new Get(TestBytes.row));
  }

}
