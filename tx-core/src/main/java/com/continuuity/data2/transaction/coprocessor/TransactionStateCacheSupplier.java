package com.continuuity.data2.transaction.coprocessor;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;

/**
 * Supplies instances of {@link TransactionStateCache} implementations.
 */
public class TransactionStateCacheSupplier implements Supplier<TransactionStateCache> {
  protected static volatile TransactionStateCache instance;
  protected static Object lock = new Object();

  protected final Configuration conf;

  public TransactionStateCacheSupplier(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Returns a singleton instance of the transaction state cache, performing lazy initialization if necessary.
   * @return A shared instance of the transaction state cache.
   */
  @Override
  public TransactionStateCache get() {
    if (instance == null) {
      synchronized (lock) {
        if (instance == null) {
          instance = new TransactionStateCache();
          instance.setConf(conf);
          instance.startAndWait();
        }
      }
    }
    return instance;
  }
}
