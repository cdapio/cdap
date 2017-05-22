/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.proto.id.NamespaceId;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link NotificationContext}.
 */
public final class BasicNotificationContext implements NotificationContext {
  private static final Logger LOG = LoggerFactory.getLogger(BasicNotificationContext.class);

  private final DynamicDatasetCache datasetContext;

  public BasicNotificationContext(NamespaceId namespaceId, DatasetFramework dsFramework,
                                  TransactionSystemClient txSystemClient) {
    // TODO this dataset context needs a metrics context [CDAP-3114]
    // TODO this context is only used in system code. When we expose it to user code, we need to set the class loader,
    //      the owners, the runtime arguments and the metrics context.
    this.datasetContext = new MultiThreadDatasetCache(new SystemDatasetInstantiator(dsFramework),
                                                      txSystemClient, namespaceId,
                                                      null, null, null);
  }

  @Override
  public boolean execute(TxRunnable runnable, TxRetryPolicy policy) {
    int failureCount = 0;
    while (true) {
      try {
        TransactionContext context = datasetContext.newTransactionContext();
        context.start();
        try {
          runnable.run(datasetContext);
        } catch (Throwable t) {
          context.abort(new TransactionFailureException("Exception thrown from runnable. Aborting transaction.", t));
        }
        context.finish();
        return true;

      } catch (Throwable t) {
        switch (policy.handleFailure(++failureCount, t)) {
          case RETRY:
            LOG.warn("Retrying failed transactional operation", t);
            break;
          case DROP:
            LOG.warn("Failed to execute transactional operation", t);
            return false;
        }
      }
    }
  }
}
