package com.continuuity.data2.transaction.coprocessor;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.transaction.persist.HDFSTransactionStateStorage;
import com.continuuity.data2.transaction.persist.TransactionSnapshot;
import com.continuuity.data2.transaction.persist.TransactionStateStorage;
import com.continuuity.data2.transaction.snapshot.SnapshotCodecProvider;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Periodically refreshes transaction state from the latest stored snapshot.  This is implemented as a singleton
 * to allow a single cache to be shared by all regions on a regionserver.
 */
public class TransactionStateCache extends AbstractIdleService implements Configurable {
  private static final Log LOG = LogFactory.getLog(TransactionStateCache.class);

  // how frequently we should wake to check for changes (in seconds)
  private static final long CHECK_FREQUENCY = 15;

  private Configuration hConf;

  private TransactionStateStorage storage;
  private volatile TransactionSnapshot latestState;

  private Thread refreshService;
  private long lastRefresh;
  // snapshot refresh frequency in milliseconds
  private long snapshotRefreshFrequency;
  private boolean initialized;

  public TransactionStateCache() {
  }

  @Override
  public Configuration getConf() {
    return hConf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.hConf = conf;
  }

  @Override
  protected void startUp() throws Exception {
    refreshState();
    startRefreshService();
  }

  @Override
  protected void shutDown() throws Exception {
    this.refreshService.interrupt();
    this.storage.stop();
  }

  /**
   * Try to initialize the CConfiguration and TransactionStateStorage instances.  Obtaining the CConfiguration may
   * fail until ReactorServiceMain has been started.
   */
  private void tryInit() {
    try {
      CConfiguration conf = getSnapshotConfiguration();
      if (conf != null) {
        this.storage = new HDFSTransactionStateStorage(conf, hConf, new SnapshotCodecProvider(conf));
        this.storage.startAndWait();
        this.snapshotRefreshFrequency = conf.getLong(TxConstants.Manager.CFG_TX_SNAPSHOT_INTERVAL,
                                                     TxConstants.Manager.DEFAULT_TX_SNAPSHOT_INTERVAL) * 1000;
        this.initialized = true;
      } else {
        LOG.info("Could not load Continuuity configuration");
      }
    } catch (Exception e) {
      LOG.info("Failed to initialize TransactionStateCache due to: " + e.getMessage());
    }
  }

  protected CConfiguration getSnapshotConfiguration() throws IOException {
    CConfiguration conf = CConfiguration.create();
    conf.unset(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES);
    return conf;
  }

  private void reset() {
    this.storage.stop();
    this.lastRefresh = 0;
    this.initialized = false;
  }

  private void startRefreshService() {
    this.refreshService = new Thread("tx-state-refresh") {
      @Override
      public void run() {
        while (!isInterrupted()) {
          if (latestState == null || System.currentTimeMillis() > (lastRefresh + snapshotRefreshFrequency)) {
            try {
              refreshState();
            } catch (IOException ioe) {
              LOG.info("Error refreshing transaction state cache: " + ioe.getMessage());
            }
          }
          try {
            TimeUnit.SECONDS.sleep(CHECK_FREQUENCY);
          } catch (InterruptedException ie) {
            // reset status
            interrupt();
            break;
          }
        }
        LOG.info("Exiting thread " + getName());
      }
    };
    this.refreshService.setDaemon(true);
    this.refreshService.start();
  }

  private void refreshState() throws IOException {
    if (!initialized) {
      tryInit();
    }

    // only continue if initialization was successful
    if (initialized) {
      long now = System.currentTimeMillis();
      TransactionSnapshot currentSnapshot = storage.getLatestSnapshot();
      if (currentSnapshot != null) {
        if (currentSnapshot.getTimestamp() < (now - 2 * snapshotRefreshFrequency)) {
          LOG.info("Current snapshot is old, will force a refresh on next run.");
          reset();
        } else {
          latestState = currentSnapshot;
          LOG.info("Transaction state reloaded with snapshot from " + latestState.getTimestamp());
          lastRefresh = now;
        }
      } else {
        LOG.info("No transaction state found.");
      }
    }
  }

  public TransactionSnapshot getLatestState() {
    return latestState;
  }
}
