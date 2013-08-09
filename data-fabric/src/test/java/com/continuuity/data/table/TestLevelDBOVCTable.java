package com.continuuity.data.table;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.engine.leveldb.LevelDBOVCTable;
import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestLevelDBOVCTable extends TestOVCTable {

  private static final Logger LOG = LoggerFactory.getLogger(TestLevelDBOVCTable.class);

  private static CConfiguration conf;

  static {
    CConfiguration conf = CConfiguration.create();
    conf.unset(Constants.CFG_DATA_LEVELDB_DIR);
    TestLevelDBOVCTable.conf = conf;
  }
  private static final Injector injector = Guice.createInjector (
      new DataFabricLevelDBModule(conf));

  @Override
  protected OVCTableHandle injectTableHandle() {
    return injector.getInstance(OVCTableHandle.class);
  }

  @Override
  public void testInjection() {
    assertTrue(table instanceof LevelDBOVCTable);
  }

  /**
   * This test is to test opening LevelDBOVCTable with the same name from multiple threads simultaneously.
   */
  @Test
  public void testConcurrentOpen() throws InterruptedException {
    ExecutorService executorService = Executors.newFixedThreadPool(5);

    final byte[] tableName = Bytes.toBytes("testConcurrentOpen");
    final CyclicBarrier startBarrier = new CyclicBarrier(5);
    final AtomicInteger successCount = new AtomicInteger(0);

    // Start 5 threads to get the same table at the same time.
    for (int i = 0; i < 5; i++) {
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          try {
            startBarrier.await();
            tableHandle.getTable(tableName);
            successCount.incrementAndGet();
          } catch (Exception e) {
            LOG.error("Exception: {}", e.getMessage(), e);
          }
        }
      });
    }

    // Wait for all tasks to be done
    executorService.shutdown();
    executorService.awaitTermination(5, TimeUnit.SECONDS);

    Assert.assertEquals(5, successCount.get());
  }
}
