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

package co.cask.cdap.notifications.service;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DynamicDatasetContext;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implementation of {@link NotificationContext}.
 */
public final class BasicNotificationContext implements NotificationContext {
  private static final Logger LOG = LoggerFactory.getLogger(BasicNotificationContext.class);

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
          runnable.run(new DynamicDatasetContext(context, dsFramework, context.getClass().getClassLoader()) {
            @Nullable
            @Override
            protected LoadingCache<Long, Map<String, Dataset>> getDatasetsCache() {
              return null;
            }
          });
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
}
