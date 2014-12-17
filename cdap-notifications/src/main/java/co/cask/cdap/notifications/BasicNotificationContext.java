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

package co.cask.cdap.notifications;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.notifications.client.AbstractNotificationSubscriber;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Implementation of {@link NotificationContext}.
 */
public final class BasicNotificationContext implements NotificationContext {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractNotificationSubscriber.class);

  private final DatasetFramework dsFramework;
  private final TransactionSystemClient transactionSystemClient;

  public BasicNotificationContext(DatasetFramework dsFramework, TransactionSystemClient transactionSystemClient) {
    this.dsFramework = dsFramework;
    this.transactionSystemClient = transactionSystemClient;
  }

  @Override
  public boolean execute(TxRunnable runnable, TxRetryPolicy policy) {
    int countFail = 0;
    while (true) {
      try {
        final TransactionContext context = new TransactionContext(transactionSystemClient);
        try {
          context.start();
          runnable.run(new DynamicDatasetContext(context));
          context.finish();
          return true;
        } catch (TransactionFailureException e) {
          abortTransaction(e, "Failed to commit. Aborting transaction.", context);
        } catch (Exception e) {
          abortTransaction(e, "Exception occurred running user code. Aborting transaction.", context);
        }
      } catch (Throwable t) {
        switch (policy.handleFailure(++countFail, t)) {
          case RETRY:
            LOG.warn("Retrying failed transaction");
            break;
          case DROP:
            LOG.warn("Could not execute transactional operation.", t);
            return false;
        }
      }
    }
  }

  private void abortTransaction(Exception e, String message, TransactionContext context) throws Exception {
    try {
      LOG.error(message, e);
      context.abort();
      throw e;
    } catch (TransactionFailureException e1) {
      LOG.error("Failed to abort transaction.", e1);
      throw e1;
    }
  }

  /**
   * Implementation of {@link co.cask.cdap.api.data.DatasetContext} that allows to dynamically load datasets
   * into a started {@link TransactionContext}.
   */
  private final class DynamicDatasetContext implements DatasetContext {

    private final TransactionContext context;

    private DynamicDatasetContext(TransactionContext context) {
      this.context = context;
    }

    @Override
    public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
      return getDataset(name, null);
    }

    @Override
    public <T extends Dataset> T getDataset(String name, Map<String, String> arguments)
      throws DatasetInstantiationException {
      try {
        T dataset = dsFramework.getDataset(name, arguments, null);
        if (dataset instanceof TransactionAware) {
          context.addTransactionAware((TransactionAware) dataset);
        }
        return dataset;
      } catch (DatasetManagementException e) {
        throw new DatasetInstantiationException("Could not retrieve dataset metadata", e);
      } catch (IOException e) {
        throw new DatasetInstantiationException("Error when instantiating dataset", e);
      }
    }
  }
}
