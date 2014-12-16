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

package co.cask.cdap.notifications.client;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.service.TxRunnable;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.notifications.NotificationContext;
import co.cask.cdap.notifications.NotificationFeed;
import co.cask.cdap.notifications.NotificationHandler;
import co.cask.cdap.notifications.TxRetryPolicy;
import co.cask.cdap.notifications.service.NotificationFeedException;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Common implementation of a {@link co.cask.cdap.notifications.client.NotificationClient.Subscriber}.
 */
public abstract class AbstractNotificationSubscriber implements NotificationClient.Subscriber {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractNotificationSubscriber.class);

  private final NotificationFeedClient feedClient;
  private final Map<NotificationFeed, NotificationHandler> feedMap;
  private final DatasetFramework dsFramework;
  private final TransactionSystemClient transactionSystemClient;
  private boolean isConsuming;

  protected AbstractNotificationSubscriber(NotificationFeedClient feedClient, DatasetFramework dsFramework,
                                           TransactionSystemClient transactionSystemClient) {
    this.feedClient = feedClient;
    this.dsFramework = dsFramework;
    this.transactionSystemClient = transactionSystemClient;
    this.feedMap = Maps.newConcurrentMap();
    this.isConsuming = false;
  }

  protected abstract void doSubscribe(NotificationFeed feed);

  protected abstract String decodeMessageKey(ByteBuffer buffer) throws IOException;

  protected abstract String buildMessageKey(NotificationFeed feed);

  protected abstract Object decodePayload(ByteBuffer buffer, Type type) throws IOException;

  protected void startConsuming() {
    isConsuming = true;
  }

  public boolean isConsuming() {
    return isConsuming;
  }

  protected void handlePayload(ByteBuffer payload) throws IOException {
      String msgKey = decodeMessageKey(payload);
      for (Map.Entry<NotificationFeed, NotificationHandler> feedEntry : feedMap.entrySet()) {
        if (!msgKey.equals(buildMessageKey(feedEntry.getKey()))) {
          continue;
        }
        Object notification = decodePayload(payload, feedEntry.getValue().getNotificationFeedType());
        if (notification == null) {
          continue;
        }
        try {
          feedEntry.getValue().processNotification(notification, new BasicNotificationContext());
        } catch (Throwable t) {
          LOG.warn("Error while processing notification: {}", notification, t);
        }
      }

  }

  @Override
  public synchronized <N> NotificationClient.Subscriber add(NotificationFeed feed, NotificationHandler<N> handler)
    throws NotificationFeedException {
    if (isConsuming) {
      throw new NotificationFeedException("Preparer is already consuming Notifications, feeds cannot be added.");
    }
    // This call will make sure that the feed exists
    feedClient.getFeed(feed);

    doSubscribe(feed);
    feedMap.put(feed, handler);
    return this;
  }

  @Override
  public Cancellable consume() {
    return null;
  }

  /**
   * Implementation of {@link co.cask.cdap.notifications.NotificationContext}.
   */
  private final class BasicNotificationContext implements NotificationContext {

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

    private void abortTransaction(Exception e, String message, TransactionContext context) {
      try {
        LOG.error(message, e);
        context.abort();
        throw Throwables.propagate(e);
      } catch (TransactionFailureException e1) {
        LOG.error("Failed to abort transaction.", e1);
        throw Throwables.propagate(e1);
      }
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
