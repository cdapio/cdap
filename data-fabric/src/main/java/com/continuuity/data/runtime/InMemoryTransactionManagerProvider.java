package com.continuuity.data.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.metrics.TxMetricsCollector;
import com.continuuity.data2.transaction.persist.TransactionStateStorage;
import com.continuuity.http.HttpHandler;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.Set;

/**
 * Google Guice Provider for {@link InMemoryTransactionManager} instances.  Each call to {@link #get()} will
 * return a new {@link InMemoryTransactionManager} instance.
 */
public class InMemoryTransactionManagerProvider implements Provider<InMemoryTransactionManager> {
  private final CConfiguration conf;
  private final Provider<TransactionStateStorage> storageProvider;
  private final TxMetricsCollector txMetricsCollector;
  private Configuration txConfig;

  @Inject
  public InMemoryTransactionManagerProvider(CConfiguration config, Provider<TransactionStateStorage> storageProvider,
                                            TxMetricsCollector txMetricsCollector,
                                            @Named("transaction") Configuration txConfig) {
    this.conf = config;
    this.storageProvider = storageProvider;
    this.txMetricsCollector = txMetricsCollector;
    this.txConfig = txConfig;
  }

  @Override
  public InMemoryTransactionManager get() {
    return new InMemoryTransactionManager(txConfig, storageProvider.get(), txMetricsCollector);
  }
}
