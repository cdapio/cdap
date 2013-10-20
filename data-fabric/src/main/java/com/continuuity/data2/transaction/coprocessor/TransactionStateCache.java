package com.continuuity.data2.transaction.coprocessor;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.transaction.persist.HDFSTransactionStateStorage;
import com.continuuity.data2.transaction.persist.TransactionSnapshot;
import com.continuuity.data2.transaction.persist.TransactionStateStorage;
import com.continuuity.data2.util.hbase.ConfigurationTable;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Periodically refreshes transaction state from the latest stored snapshot.  This is implemented as a singleton
 * to allow a single cache to be shared by all regions on a regionserver.
 */
public class TransactionStateCache extends AbstractIdleService {
  private static final Log LOG = LogFactory.getLog(TransactionStateCache.class);

  // how frequently we should wake to check for changes (in seconds)
  private static final long CHECK_FREQUENCY = 15;
  private static volatile TransactionStateCache instance;
  private static Object lock = new Object();

  private final Configuration hConf;
  private final String tableNamespace;
  private final ConfigurationTable configTable;

  private TransactionStateStorage storage;
  private volatile TransactionSnapshot latestState;

  private Thread refreshService;
  private long lastRefresh;
  // snapshot refresh frequency in milliseconds
  private long snapshotRefreshFrequency;
  private CConfiguration conf;
  private boolean initialized;

  /**
   * Package private constructor for internal use and testing only.  To obtain a new instance, call
   * {@link #get(org.apache.hadoop.conf.Configuration, String)}.
   * @param hConf The HBase configuration to use.
   */
  TransactionStateCache(Configuration hConf, String namespace) {
    this.hConf = hConf;
    this.tableNamespace = namespace;
    this.configTable = new ConfigurationTable(hConf);
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
   * fail until OpexServiceMain has been started.
   */
  private void tryInit() {
    try {
      this.conf = configTable.read(ConfigurationTable.Type.DEFAULT, tableNamespace);
      if (conf != null) {
        this.storage = new HDFSTransactionStateStorage(conf, hConf);
        this.storage.startAndWait();
        this.snapshotRefreshFrequency = conf.getLong(Constants.Transaction.Manager.CFG_TX_SNAPSHOT_INTERVAL,
                                                     Constants.Transaction.Manager.DEFAULT_TX_SNAPSHOT_INTERVAL) * 1000;
        this.initialized = true;
      } else {
        LOG.info("Could not load Continuuity configuration");
      }
    } catch (IOException ioe) {
      LOG.info("Failed to initialize TransactionStateCache due to: " + ioe.getMessage());
    }
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

  /**
   * Returns a singleton instance of the transaction state cache, performing lazy initialization if necessary.
   * @param hConf The Hadoop configuration to use.
   * @return A shared instance of the transaction state cache.
   */
  public static TransactionStateCache get(Configuration hConf, String namespace) {
    if (instance == null) {
      synchronized (lock) {
        if (instance == null) {
          instance = new TransactionStateCache(hConf, namespace);
          instance.startAndWait();
        }
      }
    }
    return instance;
  }
}
