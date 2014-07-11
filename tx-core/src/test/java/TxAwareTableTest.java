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

  @Test
  public void testValidTransactionalPutAndGet() throws Exception {
    HTable hTable = testUtil.createTable(Bytes.toBytes("test"), Bytes.toBytes("family"));
    TxAwareTable txAwareTable = new TxAwareTable(hTable);

    TransactionContext transactionContext = new TransactionContext(new DetachedTxSystemClient(),
                                                                   txAwareTable);
    transactionContext.start();
    Put put = new Put(Bytes.toBytes("testrow"));
    put.add(Bytes.toBytes("family"), Bytes.toBytes("testqualifier"), Bytes.toBytes("testvalue"));
    txAwareTable.put(put);
    transactionContext.finish();

    Result result = txAwareTable.get(new Get(Bytes.toBytes("testrow")));
    String value = Bytes.toString(result.getValue(Bytes.toBytes("family"), Bytes.toBytes("testqualifier")));
    Assert.assertEquals("testvalue", value);
  }

  @Test
  public void testAbortedTransactionPutAndGet() throws Exception {
    HTable hTable = testUtil.createTable(Bytes.toBytes("test2"), Bytes.toBytes("family2"));
    TxAwareTable txAwareTable = new TxAwareTable(hTable);

    TransactionContext transactionContext = new TransactionContext(new DetachedTxSystemClient(),
                                                                   txAwareTable);
    transactionContext.start();
    Put put = new Put(Bytes.toBytes("testrow2"));
    put.add(Bytes.toBytes("family2"), Bytes.toBytes("testqualifier2"), Bytes.toBytes("testvalue2"));
    txAwareTable.put(put);

    transactionContext.abort();

    Result result = txAwareTable.get(new Get(Bytes.toBytes("testrow2")));
    String value = Bytes.toString(result.getValue(Bytes.toBytes("family2"), Bytes.toBytes("testqualifier2")));
    Assert.assertEquals(value, null);
  }

  @Test
  public void testValidTransactionalDelete() throws Exception {
    HTable hTable = testUtil.createTable(Bytes.toBytes("test3"), Bytes.toBytes("family3"));
    TxAwareTable txAwareTable = new TxAwareTable(hTable);

    TransactionContext transactionContext = new TransactionContext(new DetachedTxSystemClient(),
                                                                   txAwareTable);
    transactionContext.start();
    Put put = new Put(Bytes.toBytes("testrow3"));
    put.add(Bytes.toBytes("family3"), Bytes.toBytes("testqualifier3"), Bytes.toBytes("testvalue3"));
    txAwareTable.put(put);
    transactionContext.finish();

    Result result = txAwareTable.get(new Get(Bytes.toBytes("testrow3")));
    String value = Bytes.toString(result.getValue(Bytes.toBytes("family3"), Bytes.toBytes("testqualifier3")));
    Assert.assertEquals("testvalue3", value);

    transactionContext.start();
    Delete delete = new Delete(Bytes.toBytes("testrow3"));
    txAwareTable.delete(delete);

    transactionContext.finish();

    result = txAwareTable.get(new Get(Bytes.toBytes("testrow3")));
    value = Bytes.toString(result.getValue(Bytes.toBytes("family3"), Bytes.toBytes("testqualifier3")));
    Assert.assertEquals(null, value);
  }

  @Test
  public void testInvalidTransactionalDelete() throws Exception {
    HTable hTable = testUtil.createTable(Bytes.toBytes("test4"), Bytes.toBytes("family4"));
    TxAwareTable txAwareTable = new TxAwareTable(hTable);

    TransactionContext transactionContext = new TransactionContext(new DetachedTxSystemClient(),
                                                                   txAwareTable);
    transactionContext.start();
    Put put = new Put(Bytes.toBytes("testrow4"));
    put.add(Bytes.toBytes("family4"), Bytes.toBytes("testqualifier4"), Bytes.toBytes("testvalue4"));
    txAwareTable.put(put);
    transactionContext.finish();

    Result result = txAwareTable.get(new Get(Bytes.toBytes("testrow4")));
    String value = Bytes.toString(result.getValue(Bytes.toBytes("family4"), Bytes.toBytes("testqualifier4")));
    Assert.assertEquals("testvalue4", value);

    transactionContext.start();
    Delete delete = new Delete(Bytes.toBytes("testrow4"));
    txAwareTable.delete(delete);
    transactionContext.abort();

    result = txAwareTable.get(new Get(Bytes.toBytes("testrow4")));
    value = Bytes.toString(result.getValue(Bytes.toBytes("family4"), Bytes.toBytes("testqualifier4")));
    Assert.assertEquals("testvalue4", value);
  }

}
