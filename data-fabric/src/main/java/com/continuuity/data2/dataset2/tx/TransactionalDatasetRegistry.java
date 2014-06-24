package com.continuuity.data2.dataset2.tx;

import com.continuuity.api.dataset.Dataset;
import com.continuuity.data2.dataset2.DatasetManagementException;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionFailureException;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Utility that wraps the execution of a function into the context of a transaction.
 *
 * TODO: unify with TransactionExecutor as per comment in https://github.com/continuuity/reactor/pull/1243
 *
 * @param <CONTEXT_TYPE> type of the tx operation context
 */
public abstract class TransactionalDatasetRegistry<CONTEXT_TYPE extends TxContext> extends AbstractIdleService {
  private final TransactionSystemClient txClient;

  public TransactionalDatasetRegistry(TransactionSystemClient txClient) {
    this.txClient = txClient;
  }

  public <RETURN_TYPE> RETURN_TYPE execute(final TxCallable<CONTEXT_TYPE, RETURN_TYPE> tx)
    throws TransactionFailureException, IOException, DatasetManagementException, InterruptedException {

    final CONTEXT_TYPE context = createContext();
    Map<String, ? extends Dataset> datasets = context.getDatasets();

    List<TransactionAware> txAwares = Lists.newArrayList();
    for (Dataset dataset: datasets.values()) {
      if (dataset instanceof TransactionAware) {
        txAwares.add((TransactionAware) dataset);
      }
    }

    TransactionExecutor txExecutor = new DefaultTransactionExecutor(txClient, txAwares);

    RETURN_TYPE result = txExecutor.execute(new Callable<RETURN_TYPE>() {
      @Override
      public RETURN_TYPE call() throws Exception {
        return tx.call(context);
      }
    });

    for (Dataset dataset: datasets.values()) {
      dataset.close();
    }

    return result;
  }

  public <RETURN_TYPE> RETURN_TYPE executeUnchecked(final TxCallable<CONTEXT_TYPE, RETURN_TYPE> tx) {
    try {
      return execute(tx);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  protected abstract CONTEXT_TYPE createContext() throws IOException, DatasetManagementException;
}
