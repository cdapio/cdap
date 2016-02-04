/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data2.dataset2.tx;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.common.service.UncaughtExceptionIdleService;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.transaction.TransactionSystemClientService;
import co.cask.tephra.DefaultTransactionExecutor;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionFailureException;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Utility that wraps the execution of a function into the context of a transaction.
 *
 * TODO: unify with TransactionExecutor as per comment in https://github.co.cask.cdap/reactor/pull/1243
 *
 * @param <CONTEXT_TYPE> type of the tx operation context
 */
public abstract class TransactionalDatasetRegistry<CONTEXT_TYPE extends TxContext>
  extends UncaughtExceptionIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionalDatasetRegistry.class);
  private final TransactionSystemClientService txClient;

  public TransactionalDatasetRegistry(TransactionSystemClientService txClient) {
    this.txClient = txClient;
  }

  @Override
  protected void startUp() throws Exception {
    txClient.startAndWait();
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

  @Override
  protected Logger getUncaughtExceptionLogger() {
    return LOG;
  }
}
