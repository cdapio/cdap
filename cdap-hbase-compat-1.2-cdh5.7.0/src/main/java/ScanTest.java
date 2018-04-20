import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class ScanTest implements Runnable {
  private static final byte[] FAMILY = {'c'};
  private static final TableName SCAN_TEST_TABLE = TableName.valueOf("scan_test");
  private static final int NUM_SPLITS = 16;

  private final CountDownLatch startLatch = new CountDownLatch(0);
  private volatile Connection connection;
  private static volatile int numRows;

  public static void main(String[] args)
    throws IOException, InterruptedException, ExecutionException, TimeoutException {

    int numThreads = 10;
    numRows = 1000;

    if (args.length > 0) {
      numThreads = Integer.parseInt(args[0]);
    }
    if (args.length > 1) {
      numRows = Integer.parseInt(args[1]);
    }

    ScanTest scanTest = new ScanTest();
    scanTest.initialize();
    try {
      System.out.println(String.format("Running scan test with num-threads = %s and num-rows = %s...",
                                       numThreads, numRows));
//      scanTest.createTable(SCAN_TEST_TABLE);
      try {
//        scanTest.populateData(SCAN_TEST_TABLE, numRows);
        scanTest.runTest(numThreads);
      } finally {
        System.out.println("Not deleting the table");
//        scanTest.deleteTable(SCAN_TEST_TABLE);
      }
      System.out.println("Scan test done.");
    } finally {
      scanTest.destroy();
    }
  }

  private void initialize() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    connection = ConnectionFactory.createConnection(conf);
  }

  private void destroy () throws IOException {
    connection.close();
  }

  private void runTest(int numThreads) throws InterruptedException, ExecutionException, TimeoutException {
    ListeningExecutorService executor =
      MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(numThreads,
                                     new ThreadFactoryBuilder().setNameFormat("scan-test-%d").build()));
    try {
      List<ListenableFuture<?>> futures = new ArrayList<>();
      for (int i = 0; i < numThreads; i++) {
        futures.add(executor.submit(this));
      }
      System.out.println("Starting " + numThreads +  " scan threads");
      startLatch.countDown();

      Futures.allAsList(futures).get(3, TimeUnit.MINUTES);
    } finally {
      executor.shutdownNow();
    }
    System.out.println("Done with all threads");
  }

  @Override
  public void run() {
    try {
      if (!startLatch.await(30, TimeUnit.SECONDS)) {
        System.out.println("Timed out waiting to start!");
      }

      int count = 0;
      try (Table table = connection.getTable(SCAN_TEST_TABLE)) {
        Scan scan = new Scan();
        try (ResultScanner scanner = table.getScanner(scan)) {
          while (scanner.next() != null) {
            ++count;
          }
        }
      }
      System.out.println(String.format("Thread = %s, expected %s, actual = %s, result = %s",
                                       Thread.currentThread().getName(), numRows, count, numRows == count));
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void createTable(TableName tableName) throws IOException {
    try (Admin admin = this.connection.getAdmin()) {
      if (admin.tableExists(tableName)) {
        System.out.println("Table " + tableName + " already exists!");
        return;
      }

      byte[][] splits = new byte[NUM_SPLITS][];
      for (int i = 0; i < NUM_SPLITS; i++) {
        splits[i] = new byte[] {(byte) i};
      }

      HTableDescriptor htd = new HTableDescriptor(tableName);
      htd.addFamily(new HColumnDescriptor(FAMILY).setMaxVersions(1));
      admin.createTable(htd, splits);
      System.out.println("Created table " + tableName.getNameWithNamespaceInclAsString());
    } catch (TableExistsException ex) {
      System.out.println("Not creating table since it already exists: " +
                tableName.getNameWithNamespaceInclAsString());
    }
  }

  private void deleteTable(TableName tableName) throws IOException {
    try (Admin admin = this.connection.getAdmin()) {
      if (!admin.tableExists(tableName)) {
        System.out.println("Table " + tableName + " is already deleted!");
        return;
      }

      admin.disableTable(tableName);
      admin.deleteTable(tableName);
      System.out.println("Deleted table " + tableName);
    }
  }

  private void populateData(TableName tableName, int numRows) throws IOException {
    byte[] qualifier = {'q'};
    byte[] value = {'v'};

    int batchSize = Math.min(2000, numRows);
    int numBatches = 0;
    System.out.println("Adding " + numRows + " rows to table " + tableName + " with batch size " + batchSize);
    try (Table table = connection.getTable(tableName)) {
      List<Put> puts = new ArrayList<>(batchSize);
      for (int i = 0; i < numRows;) {
        numBatches++;
        puts.clear();
        for (int j = 0; j < batchSize && i < numRows; j++) {
          i++;
          // second part is the split key
          Put put = new Put(Bytes.add(new byte[] {(byte) (i % NUM_SPLITS)}, Bytes.toBytes(i)));
          put.addColumn(FAMILY, qualifier, value);
          puts.add(put);
        }
        table.put(puts);
      }
    }
    System.out.println("Added " + numRows + " rows to table " + tableName + " in " + numBatches + " batches");
  }
}
