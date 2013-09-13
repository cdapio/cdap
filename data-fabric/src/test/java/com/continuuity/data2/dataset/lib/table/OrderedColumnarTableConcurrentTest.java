package com.continuuity.data2.dataset.lib.table;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * This test emulates usage table by multiple concurrent clients
 * @param <T> table type
 */
public abstract class OrderedColumnarTableConcurrentTest<T extends OrderedColumnarTable>
  extends OrderedColumnarTableTest<T> {

  private static final byte[] ROW_TO_INCREMENT = Bytes.toBytes("row_to_increment");
  private static final byte[] COLUMN_TO_INCREMENT = Bytes.toBytes("column_to_increment");

  protected TransactionExecutorFactory txExecutorFactory;

  protected abstract T getTable(String name) throws Exception;
  protected abstract DataSetManager getTableManager() throws Exception;

  @Before
  public void before() {
    super.before();
    txExecutorFactory = new TransactionExecutorFactory() {
      @Override
      public DefaultTransactionExecutor createExecutor(Iterable<TransactionAware> txAwares) {
        return new DefaultTransactionExecutor(txClient, txAwares);
      }
    };

  }

  @Test
  public void testSingleTable() throws Exception {
    // Set of clients read and write data concurrently.
    // * 10 clients increment a value with increasing values (+1, +2, ...) at specific row:column 100 times
    // * todo: 10 clients append 100 columns to a set of 10 rows. Append is: read all columns, add <last_column+1>

    getTableManager().create("myTable");
    try {

      final Thread[] incrementingClients = new Thread[10];
      for (int i = 0; i < incrementingClients.length; i++) {
        incrementingClients[i] = new Thread(new IncrementingClient(txExecutorFactory));
      }

      // start threads
      for (Thread incrementingClient : incrementingClients) {
        incrementingClient.start();
      }

      // wait for finish
      for (Thread incrementingClient : incrementingClients) {
        incrementingClient.join();
      }

      // verify result
      final T table = getTable("myTable");
      DefaultTransactionExecutor txExecutor =
        txExecutorFactory.createExecutor(Lists.newArrayList((TransactionAware) table));
      txExecutor.execute(new Function<Object, Object>() {
        @Nullable
        @Override
        public Object apply(@Nullable Object input) {
          try {
            OperationResult<Map<byte[],byte[]>> result = table.get(ROW_TO_INCREMENT, new byte[][]{COLUMN_TO_INCREMENT});
            Assert.assertFalse(result.isEmpty());
            byte[] val = result.getValue().get(COLUMN_TO_INCREMENT);
            long sum1_100 = ((1 + 99) * 99 / 2);
            Assert.assertEquals(incrementingClients.length * sum1_100, Bytes.toLong(val));

          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
          return null;
        }
      }, null);


    } finally {
      getTableManager().drop("myTable");
    }
  }

  private class IncrementingClient implements Runnable {
    private final TransactionExecutorFactory txExecutorFactory;
    private final T table;

    public IncrementingClient(TransactionExecutorFactory txExecutorFactory) throws Exception {
      this.txExecutorFactory = txExecutorFactory;
      this.table = getTable("myTable");
    }

    @Override
    public void run() {
      final int[] executed = {0};
      while (executed[0] < 100) {
        System.out.println("doing: " + executed[0]);
        DefaultTransactionExecutor txExecutor =
          txExecutorFactory.createExecutor(Lists.newArrayList((TransactionAware) table));
        try {
          txExecutor.execute(new Function<Object, Object>() {
            @Nullable
            @Override
            public Object apply(@Nullable Object input) {
              try {
                table.increment(ROW_TO_INCREMENT,
                                new byte[][]{COLUMN_TO_INCREMENT},
                                new long[]{(long) executed[0]});
              } catch (Exception e) {
                throw Throwables.propagate(e);
              }
              return null;
            }
          }, null);
        } catch (Throwable t) {
          // do nothing: we'll retry execution
          System.out.println("failed " + executed[0]);
          t.printStackTrace();
          continue;
        }
        executed[0]++;
      }
    }
  }

}
