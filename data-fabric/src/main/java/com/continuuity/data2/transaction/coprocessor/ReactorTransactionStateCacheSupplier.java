package com.continuuity.data2.transaction.coprocessor;

import org.apache.hadoop.conf.Configuration;

/**
 * Provides a single shared instance of
 * {@link com.continuuity.data2.transaction.coprocessor.ReactorTransactionStateCache} for use by transaction
 * coprocessors.
 */
public class ReactorTransactionStateCacheSupplier extends TransactionStateCacheSupplier {
  private final String namespace;

  public ReactorTransactionStateCacheSupplier(String namespace, Configuration conf) {
    super(conf);
    this.namespace = namespace;
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
          instance = new ReactorTransactionStateCache(namespace);
          instance.setConf(conf);
          instance.startAndWait();
        }
      }
    }
    return instance;
  }

}
