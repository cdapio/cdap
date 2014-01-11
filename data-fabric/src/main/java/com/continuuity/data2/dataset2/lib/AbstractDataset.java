package com.continuuity.data2.dataset2.lib;

import com.continuuity.api.data.dataset2.Dataset;
import com.continuuity.api.data.dataset2.metrics.MeteredDataset;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionAwares;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Collection;

/**
 * Handy abstract implementation of {@link Dataset} that acts on a list of underlying datasets and
 * implements {@link TransactionAware} and {@link MeteredDataset} interfaces by propagating corresponded
 * logic to each dataset in a list when possible.
 */
public abstract class AbstractDataset implements Dataset, MeteredDataset, TransactionAware {
  private final Collection<Dataset> underlying;
  private final TransactionAware txAwares;

  public AbstractDataset(Dataset... underlying) {
    this.underlying = Lists.newArrayList(underlying);
    ImmutableList.Builder<TransactionAware> builder = ImmutableList.builder();
    for (Dataset dataset : underlying) {
      if (dataset instanceof TransactionAware) {
        builder.add((TransactionAware) dataset);
      }
    }
    this.txAwares = TransactionAwares.of(builder.build());
  }

  @Override
  public void close() throws IOException {
    for (Dataset dataset : underlying) {
      dataset.close();
    }
  }

  // metering stuff

  @Override
  public void setMetricsCollector(MeteredDataset.MetricsCollector metricsCollector) {
    for (Dataset dataset : underlying) {
      if (dataset instanceof MeteredDataset) {
        ((MeteredDataset) dataset).setMetricsCollector(metricsCollector);
      }
    }
  }

  // transaction stuff

  @Override
  public void startTx(Transaction tx) {
    txAwares.startTx(tx);
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    return txAwares.getTxChanges();
  }

  @Override
  public boolean commitTx() throws Exception {
    return txAwares.commitTx();
  }

  @Override
  public void postTxCommit() {
    txAwares.postTxCommit();
  }

  @Override
  public boolean rollbackTx() throws Exception {
    return txAwares.rollbackTx();
  }

  @Override
  public String getName() {
    return txAwares.getName();
  }
}
