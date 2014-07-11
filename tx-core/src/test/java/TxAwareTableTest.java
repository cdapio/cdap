import com.continuuity.api.common.Bytes;
import com.continuuity.data2.transaction.TransactionContext;
import com.continuuity.data2.transaction.TxAwareTable;
import com.continuuity.data2.transaction.inmemory.DetachedTxSystemClient;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class TxAwareTableTest {
  private static HBaseTestingUtility testUtil;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    testUtil = new HBaseTestingUtility();
    testUtil.startMiniCluster();
  }

  @AfterClass
  public static void shutdownAfterClass() throws Exception {
    testUtil.shutdownMiniCluster();
  }

  private static final class TestBytes {
    private static final byte[] family = Bytes.toBytes("testfamily");
    private static final byte[] qualifier = Bytes.toBytes("testqualifier");
    private static final byte[] row = Bytes.toBytes("testrow");
    private static final byte[] value = Bytes.toBytes("testvalue");
  }

  @Test
  public void testValidTransactionalPutAndGet() throws Exception {
    HTable hTable = testUtil.createTable(Bytes.toBytes("testtable1"), TestBytes.family);
    TxAwareTable txAwareTable = new TxAwareTable(hTable);

    TransactionContext transactionContext = new TransactionContext(new DetachedTxSystemClient(),
                                                                   txAwareTable);
    transactionContext.start();
    Put put = new Put(TestBytes.row);
    put.add(TestBytes.family, TestBytes.qualifier, TestBytes.value);
    txAwareTable.put(put);
    transactionContext.finish();

    Result result = txAwareTable.get(new Get(TestBytes.row));
    byte[] value = result.getValue(TestBytes.family, TestBytes.qualifier);
    Assert.assertArrayEquals(TestBytes.value, value);
  }

  @Test
  public void testAbortedTransactionPutAndGet() throws Exception {
    HTable hTable = testUtil.createTable(Bytes.toBytes("testtable2"), TestBytes.family);
    TxAwareTable txAwareTable = new TxAwareTable(hTable);

    TransactionContext transactionContext = new TransactionContext(new DetachedTxSystemClient(),
                                                                   txAwareTable);
    transactionContext.start();
    Put put = new Put(TestBytes.row);
    put.add(TestBytes.family, TestBytes.qualifier, TestBytes.value);
    txAwareTable.put(put);

    transactionContext.abort();

    Result result = txAwareTable.get(new Get(TestBytes.row));
    byte[] value = result.getValue(TestBytes.family, TestBytes.qualifier);
    Assert.assertArrayEquals(value, null);
  }

  @Test
  public void testValidTransactionalDelete() throws Exception {
    HTable hTable = testUtil.createTable(Bytes.toBytes("testtable3"), TestBytes.family);
    TxAwareTable txAwareTable = new TxAwareTable(hTable);

    TransactionContext transactionContext = new TransactionContext(new DetachedTxSystemClient(),
                                                                   txAwareTable);
    transactionContext.start();
    Put put = new Put(TestBytes.row);
    put.add(TestBytes.family, TestBytes.qualifier, TestBytes.value);
    txAwareTable.put(put);
    transactionContext.finish();

    Result result = txAwareTable.get(new Get(TestBytes.row));
    byte[] value =result.getValue(TestBytes.family, TestBytes.qualifier);
    Assert.assertEquals(TestBytes.value, value);

    transactionContext.start();
    Delete delete = new Delete(TestBytes.row);
    txAwareTable.delete(delete);

    transactionContext.finish();

    result = txAwareTable.get(new Get(TestBytes.row));
    value = result.getValue(TestBytes.family, TestBytes.qualifier);
    Assert.assertArrayEquals(value, null);
  }

  @Test
  public void testAbortedTransactionalDelete() throws Exception {
    HTable hTable = testUtil.createTable(Bytes.toBytes("testtable4"), TestBytes.family);
    TxAwareTable txAwareTable = new TxAwareTable(hTable);

    TransactionContext transactionContext = new TransactionContext(new DetachedTxSystemClient(),
                                                                   txAwareTable);
    transactionContext.start();
    Put put = new Put(TestBytes.row);
    put.add(TestBytes.family, TestBytes.qualifier, TestBytes.value);
    txAwareTable.put(put);
    transactionContext.finish();

    Result result = txAwareTable.get(new Get(TestBytes.row));
    byte[] value = result.getValue(TestBytes.family, TestBytes.qualifier);
    Assert.assertEquals(TestBytes.value, value);

    transactionContext.start();
    Delete delete = new Delete(TestBytes.row);
    txAwareTable.delete(delete);
    transactionContext.abort();

    result = txAwareTable.get(new Get(TestBytes.row));
    value = result.getValue(TestBytes.family, TestBytes.qualifier);
    Assert.assertArrayEquals(TestBytes.value, value);
  }

}
