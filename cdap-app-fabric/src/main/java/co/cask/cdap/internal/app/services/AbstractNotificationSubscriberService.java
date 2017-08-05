/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.TxCallable;
import co.cask.cdap.internal.app.runtime.messaging.MultiThreadMessagingContext;
import co.cask.cdap.internal.app.store.AppMetadataStore;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract class that fetches notifications from TMS
 */
public abstract class AbstractNotificationSubscriberService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractNotificationSubscriberService.class);
  // Sampling log only log once per 10000
  private static final Logger SAMPLING_LOG = Loggers.sampling(LOG, LogSamplers.limitRate(10000));
  // These attributes are used to fetch the AppMetadataStore
  private static final DatasetId APP_META_INSTANCE_ID = NamespaceId.SYSTEM.dataset(Constants.AppMetaStore.TABLE);
  private static final byte[] APP_VERSION_UPGRADE_KEY = Bytes.toBytes("version.default.store");

  private static final Gson GSON = new Gson();

  private final CConfiguration cConf;
  private final Transactional transactional;
  private final MultiThreadMessagingContext messagingContext;
  private final AtomicBoolean upgradeComplete;
  private final DatasetFramework datasetFramework;
  private volatile boolean stopping = false;

  @Inject
  protected AbstractNotificationSubscriberService(MessagingService messagingService, CConfiguration cConf,
                                                  DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this.cConf = cConf;
    this.datasetFramework = datasetFramework;
    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), new TransactionSystemClientAdapter(txClient),
        NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
    this.upgradeComplete = new AtomicBoolean(false);
  }

  @Override
  protected void shutDown() {
    stopping = true;
  }

  /**
   * Thread that subscribes to TMS notifications on a specified topic and fetches notifications, retrying if necessary
   */
  protected abstract class NotificationSubscriberThread implements Runnable {
    private final String topic;
    private final RetryStrategy retryStrategy;
    private int failureCount;
    private String messageId;

    protected NotificationSubscriberThread(String topic) {
      this.topic = topic;
      // TODO: [CDAP-11370] Need to be configured in cdap-default.xml. Retry with delay ranging from 0.1s to 30s
      retryStrategy =
        co.cask.cdap.common.service.RetryStrategies.exponentialDelay(100, 30000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
      // Fetch the last processed message for the topic
      messageId = Transactions.executeUnchecked(transactional, new TxCallable<String>() {
        @Override
        public String call(DatasetContext context) throws Exception {
          return loadMessageId(context);
        }
      });

      while (!stopping) {
        try {
          long sleepTime = processNotifications();
          // Don't sleep if sleepTime returned is 0
          if (sleepTime > 0) {
            TimeUnit.MILLISECONDS.sleep(sleepTime);
          }
        } catch (InterruptedException e) {
          // sleep is interrupted, just exit without doing anything
        }
      }
    }

    /**
     * Fetches new notifications and configures time for next fetch
     *
     * @return sleep time in milliseconds before next fetch
     */
    long processNotifications() {
      try {
        // Gather a local copy of fetched notifications to batch process in one transaction
        final List<Notification> fetchedNotifications = new ArrayList<>();
        String lastFetchedMessageId = null;
        try (CloseableIterator<Message> iterator = messagingContext.getMessageFetcher()
                                                                   .fetch(NamespaceId.SYSTEM.getNamespace(), topic,
                                                                          100, messageId)) {
          LOG.trace("Fetch with messageId = {}", messageId);
          while (iterator.hasNext() && !stopping) {
            Message message = iterator.next();
            lastFetchedMessageId = message.getId();
            try {
              Notification notification = GSON.fromJson(new String(message.getPayload(), StandardCharsets.UTF_8),
                                                        Notification.class);
              fetchedNotifications.add(notification);
            } catch (JsonSyntaxException e) {
              LOG.warn("Failed to decode message with id {}. Skipped. ", message.getId(), e);
              // lastFetchedMessageId was updated at the beginning of the loop to skip this message in next fetch
            }
          }
        }

        // Sleep for configured number of milliseconds if there are no notifications
        if (lastFetchedMessageId == null) {
          return cConf.getLong(Constants.Scheduler.EVENT_POLL_DELAY_MILLIS);
        }

        final String lastMessageId = lastFetchedMessageId;
        // Execute processed notifications in one transaction
        Retries.supplyWithRetries(new Supplier<Void>() {
          @Override
          public Void get() {
            Transactions.executeUnchecked(transactional, new TxCallable<Void>() {
              @Override
              public Void call(DatasetContext context) throws Exception {
                processFetchedNotifications(context, fetchedNotifications);
                updateMessageId(context, lastMessageId);
                return null;
              }
            });
            return null;
          }
        }, co.cask.cdap.common.service.RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS,
                                                                TimeUnit.SECONDS));

        // Transaction was successful, so reset the failure count to 0
        failureCount = 0;
        // The last fetched message id was persisted successfully, update the local field as well.
        messageId = lastMessageId;
        return 0L;

      } catch (ServiceUnavailableException e) {
        SAMPLING_LOG.warn("Failed to contact service {}. Will retry in next run.", e.getServiceName(), e);
      } catch (TopicNotFoundException e) {
        SAMPLING_LOG.warn("Failed to fetch from TMS. Will retry in next run.", e);
      } catch (Exception e) {
        LOG.warn("Failed to get and process notifications. Will retry in next run", e);
      }

      // If there is any failure during fetching of notifications or looking up of schedules,
      // delay the next fetch based on the strategy
      // Exponential strategy doesn't use the time component, so doesn't matter what we passed in as startTime
      return retryStrategy.nextRetry(++failureCount, 0);
    }

    /**
     * Processes a list of notifications
     *
     * @param context the dataset context
     * @param notifications the list of pairs of message ids and fetched notifications from TMS
     * @throws Exception
     */
    void processFetchedNotifications(DatasetContext context, List<Notification> notifications)
      throws IOException, DatasetManagementException {

      for (Notification notification : notifications) {
        processNotification(context, notification);
      }
    }

    AppMetadataStore getAppMetadataStore(DatasetContext context) {
      // TODO Find a way to access the appMetadataStore without copying code from the DefaultStore

      try {
        Table table = DatasetsUtil.getOrCreateDataset(context, datasetFramework, APP_META_INSTANCE_ID,
                                                      Table.class.getName(), DatasetProperties.EMPTY);
        AppMetadataStore appMetadataStore = new AppMetadataStore(table, cConf, upgradeComplete);
        // If upgrade was not complete, check if it is and update boolean
        if (!upgradeComplete.get()) {
          boolean isUpgradeComplete = appMetadataStore.isUpgradeComplete(APP_VERSION_UPGRADE_KEY);
          if (isUpgradeComplete) {
            upgradeComplete.set(true);
          }
        }
        return appMetadataStore;
      } catch (DatasetManagementException | IOException e) {
        throw Throwables.propagate(e);
      }
    }

    /**
     * Loads the message id from storage. Note that this method is already executed inside a transaction.
     *
     * @return the message id, or null if no message id has been stored
     * @param context
     */
    public abstract String loadMessageId(DatasetContext context);

    /**
     * Persists the message id to storage. Note that this method is already executed inside a transaction.
     */
    public abstract void updateMessageId(DatasetContext context, String lastFetchedMessageId);

    /**
     * Processes the notification
     *
     * @param context the dataset context
     * @param notification the notification
     * @throws IOException
     * @throws DatasetManagementException
     */
    public abstract void processNotification(DatasetContext context, Notification notification) throws IOException,
      DatasetManagementException;
  }
}
