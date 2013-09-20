/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data.operation.executor.omid;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of an {@link com.continuuity.data.operation.executor.OperationExecutor}
 * that executes all operations within Omid-style transactions.
 *
 * See https://github.com/yahoo/omid/ for more information on the Omid design.
 */
@Singleton
public class OmidTransactionalOperationExecutor implements OperationExecutor {

  private static final Logger LOG
    = LoggerFactory.getLogger(OmidTransactionalOperationExecutor.class);

  // Metrics collectors
  private MetricsCollector txSystemMetrics;

  Thread txSystemMetricsReporter;
  private final InMemoryTransactionManager txManager;

  @Inject
  public OmidTransactionalOperationExecutor(InMemoryTransactionManager txManager, CConfiguration conf) {
    this.txManager = txManager;
    this.txSystemMetrics = createNoopMetricsCollector();
    startTxSystemMetricsReporter();
  }

  // Optional injection of MetricsCollectionService
  @Inject(optional = true)
  void setMetricsCollectionService(MetricsCollectionService metricsCollectionService) {
    this.txSystemMetrics = metricsCollectionService.getCollector(MetricsScope.REACTOR, "-.tx", "0");
  }

  private MetricsCollector createNoopMetricsCollector() {
    return new MetricsCollector() {
      @Override
      public void gauge(String metricName, int value, String... tags) {
        // No-op
      }
    };
  }

  @Override
  public com.continuuity.data2.transaction.Transaction startLong() throws OperationException {
    txSystemMetrics.gauge("tx.start.ops", 1);
    return txManager.startLong();
  }

  @Override
  public com.continuuity.data2.transaction.Transaction startShort() throws OperationException {
    txSystemMetrics.gauge("tx.start.ops", 1);
    return txManager.startShort();
  }

  @Override
  public com.continuuity.data2.transaction.Transaction startShort(int timeout) throws OperationException {
    txSystemMetrics.gauge("tx.start.ops", 1);
    return txManager.startShort(timeout);
  }

  @Override
  public boolean canCommit(com.continuuity.data2.transaction.Transaction tx, Collection<byte[]> changeIds)
    throws OperationException {
    txSystemMetrics.gauge("tx.canCommit.ops", 1);
    boolean canCommit = txManager.canCommit(tx, changeIds);
    if (canCommit) {
      txSystemMetrics.gauge("tx.canCommit.successful", 1);
    }
    return canCommit;
  }

  @Override
  public boolean commit(com.continuuity.data2.transaction.Transaction tx) throws OperationException {
    txSystemMetrics.gauge("tx.commit.ops", 1);
    boolean committed = txManager.commit(tx);
    if (committed) {
      txSystemMetrics.gauge("tx.commit.successful", 1);
    }
    return committed;
  }

  @Override
  public void abort(com.continuuity.data2.transaction.Transaction tx) throws OperationException {
    txSystemMetrics.gauge("tx.abort.ops", 1);
    txManager.abort(tx);
    txSystemMetrics.gauge("tx.abort.successful", 1);
  }

  @Override
  public void invalidate(com.continuuity.data2.transaction.Transaction tx) throws OperationException {
    txSystemMetrics.gauge("tx.invalidate.ops", 1);
    txManager.invalidate(tx);
    txSystemMetrics.gauge("tx.invalidate.successful", 1);
  }

  public void shutdown() {
    if (txSystemMetricsReporter != null) {
      txSystemMetricsReporter.interrupt();
    }
  }

  // this is a hack for reporting gauge metric: current metrics system supports only counters that are aggregated on
  // 10-sec basis, so we need to report gauge not more frequently than every 10 sec.
  private void startTxSystemMetricsReporter() {
    txSystemMetricsReporter = new Thread("tx-reporter") {
      @Override
      public void run() {
        while (!isInterrupted()) {
          int excludedListSize = txManager.getExcludedListSize();
          if (txSystemMetrics != null && excludedListSize > 0) {
            txSystemMetrics.gauge("tx.excluded", excludedListSize);
          }
          txManager.logStatistics();
          try {
            TimeUnit.SECONDS.sleep(10);
          } catch (InterruptedException e) {
            this.interrupt();
            break;
          }
        }
      }
    };
    txSystemMetricsReporter.setDaemon(true);
    txSystemMetricsReporter.start();
  }
}
