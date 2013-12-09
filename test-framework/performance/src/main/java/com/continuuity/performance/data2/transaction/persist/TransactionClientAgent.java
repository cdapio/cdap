package com.continuuity.performance.data2.transaction.persist;

import com.continuuity.data2.transaction.persist.TransactionEdit;
import com.continuuity.data2.transaction.persist.TransactionLog;
import com.continuuity.performance.benchmark.Agent;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class TransactionClientAgent extends Agent {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionClientAgent.class);
  private TransactionLog storage;
  private Supplier<TransactionEdit> editSupplier;
  private ClientMetrics metrics;
  private int batchSize;
  private List<TransactionEdit> batch;

  public TransactionClientAgent(int agentId, int batchSize, TransactionLog storage,
                                Supplier<TransactionEdit> editSupplier, ClientMetrics metrics) {
    super(agentId);
    this.batchSize = batchSize;
    this.batch = Lists.newArrayListWithCapacity(batchSize);
    this.storage = storage;
    this.editSupplier = editSupplier;
    this.metrics = metrics;
  }

  public long runOnce(long iteration) {
    try {
      for (int i = 0; i < batchSize; i++) {
        batch.add(editSupplier.get());
      }
      metrics.startTransaction();
      storage.append(batch);
      metrics.finishTransaction();
    } catch (IOException ioe) {
      LOG.warn("Batch failed: " + ioe.getMessage());
      metrics.failTransaction();
    } finally {
      batch.clear();
    }
    return batchSize;
  }

  public ClientMetrics getMetrics() {
    return metrics;
  }
}
