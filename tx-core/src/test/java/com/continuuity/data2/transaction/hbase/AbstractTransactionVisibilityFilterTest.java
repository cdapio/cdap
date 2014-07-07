package com.continuuity.data2.transaction.hbase;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.KeyValue;
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
    CConfiguration conf = CConfiguration.create();
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
