package com.continuuity.data2.transaction.coprocessor;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.transaction.persist.HDFSTransactionStateStorage;
import com.continuuity.data2.transaction.persist.TransactionSnapshot;
import com.continuuity.data2.transaction.persist.TransactionStateStorage;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Periodically refreshes transaction state from the latest stored snapshot.  This is implemented as a singleton
 * to allow a single cache to be shared by all regions on a regionserver.
 */
public class TransactionStateCache extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionStateCache.class);

  private static volatile TransactionStateCache instance;
  private static Object lock = new Object();

  private final TransactionStateStorage storage;
  private volatile TransactionSnapshot latestState;

  private final long snapshotRefreshFrequency;
  private AbstractExecutionThreadService refreshService;
  private long lastRefresh;

  /**
   * Package private constructor for internal use and testing only.  To obtain a new instance, call
   * {@link #get(org.apache.hadoop.conf.Configuration)}.
   * @param conf The Continuuity configuration to use.
   * @param storage The transaction persistence implementation.
   */
  TransactionStateCache(CConfiguration conf, TransactionStateStorage storage) {
    this.storage = storage;
    this.snapshotRefreshFrequency = conf.getLong(Constants.Transaction.Manager.CFG_TX_SNAPSHOT_INTERVAL,
                                                 Constants.Transaction.Manager.DEFAULT_TX_SNAPSHOT_INTERVAL);
  }

  @Override
  protected void startUp() throws Exception {
    this.storage.startAndWait();
    refreshState();
    startRefreshService();
  }

  @Override
  protected void shutDown() throws Exception {
    this.refreshService.stopAndWait();
    this.storage.stopAndWait();
  }

  private void startRefreshService() {
    this.refreshService = new AbstractExecutionThreadService() {
      @Override
      protected void run() throws Exception {
        if (latestState == null || System.currentTimeMillis() > (lastRefresh + snapshotRefreshFrequency)) {
          refreshState();
          TimeUnit.SECONDS.sleep(1);
        }
      }
    };
    this.refreshService.startAndWait();
  }

  private void refreshState() throws IOException {
    long now = System.currentTimeMillis();
    latestState = storage.getLatestSnapshot();
    if (latestState != null) {
      LOG.info("Transaction state reloaded with snapshot from " + latestState.getTimestamp());
      lastRefresh = now;
    } else {
      LOG.info("No transaction state found.");
    }
  }

  public TransactionSnapshot getLatestState() {
    return latestState;
  }

  /**
   * Returns a singleton instance of the transaction state cache, performing lazy initialization if necessary.
   * @param hConf The Hadoop configuration to use.
   * @return A shared instance of the transaction state cache.
   */
  public static TransactionStateCache get(Configuration hConf) {
    if (instance == null) {
      synchronized (lock) {
        if (instance == null) {
          CConfiguration conf = CConfiguration.create();
          TransactionStateStorage storage = new HDFSTransactionStateStorage(conf, hConf);
          instance = new TransactionStateCache(conf, storage);
          instance.startAndWait();
        }
      }
    }
    return instance;
  }
}
