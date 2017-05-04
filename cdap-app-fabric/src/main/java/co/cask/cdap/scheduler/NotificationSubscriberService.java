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

package co.cask.cdap.scheduler;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.MessageFetcher;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.TxCallable;
import co.cask.cdap.internal.app.runtime.messaging.MultiThreadMessagingContext;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.ScheduleTaskRunner;
import co.cask.cdap.internal.app.runtime.schedule.store.ProgramScheduleStoreDataset;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.internal.app.services.PropertiesResolver;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Subscribe to notification TMS topic and update schedules in schedule store and job queue
 */
public class NotificationSubscriberService extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(NotificationSubscriberService.class);
  // Sampling log only log once per 10000
  private static final Logger SAMPLING_LOG = Loggers.sampling(LOG, LogSamplers.limitRate(10000));
  private static final Gson GSON = new Gson();

  private final List<NotificationSubscriberThread> subscriberThreads;
  private final Transactional transactional;
  private final MultiThreadMessagingContext messagingContext;
  private final DatasetFramework datasetFramework;
  private final Store store;
  private final ProgramLifecycleService lifecycleService;
  private final PropertiesResolver propertiesResolver;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final CConfiguration cConf;
  private ScheduleTaskRunner taskRunner;
  private ListeningExecutorService taskExecutorService;

  @Inject
  NotificationSubscriberService(MessagingService messagingService,
                                Store store,
                                ProgramLifecycleService lifecycleService, PropertiesResolver propertiesResolver,
                                NamespaceQueryAdmin namespaceQueryAdmin,
                                CConfiguration cConf,
                                DatasetFramework datasetFramework,
                                TransactionSystemClient txClient) {
    this.store = store;
    this.lifecycleService = lifecycleService;
    this.propertiesResolver = propertiesResolver;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.cConf = cConf;
    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), txClient,
        NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null, this.messagingContext)),
      RetryStrategies.retryOnConflict(20, 100)
    );
    this.datasetFramework = datasetFramework;
    this.subscriberThreads = new ArrayList<>();
  }

  @Override
  protected void startUp() throws Exception {
    taskExecutorService = MoreExecutors.listeningDecorator(
      Executors.newCachedThreadPool(Threads.createDaemonThreadFactory("notification-subscriber-task")));
    taskRunner = new ScheduleTaskRunner(store, lifecycleService, propertiesResolver,
                                        taskExecutorService, namespaceQueryAdmin, cConf);
  }

  @Override
  protected void run() {
    LOG.info("Start running NotificationSubscriberService");
    if (!isRunning()) {
      return;
    }

    // TODO: (CDAP-11407) Need to read starting messageId's from a store
    subscriberThreads.add(new TimeEventNotificationSubscriberThread(null));
    subscriberThreads.add(new DataEventNotificationSubscriberThread(null));

    for (NotificationSubscriberThread thread : subscriberThreads) {
      thread.start();
    }

    for (NotificationSubscriberThread thread : subscriberThreads) {
      Uninterruptibles.joinUninterruptibly(thread);
    }
  }

  @Override
  protected void triggerShutdown() {
    LOG.info("Stopping NotificationSubscriberService.");
    for (NotificationSubscriberThread thread : subscriberThreads) {
      thread.interrupt();
    }

    LOG.info("NotificationSubscriberService stopped.");
  }

  @Override
  protected void shutDown() throws Exception {
    if (taskExecutorService != null) {
      taskExecutorService.shutdownNow();
    }
  }

  private abstract class NotificationSubscriberThread extends Thread {
    private final String topic;
    private final RetryStrategy scheduleStrategy;
    private final Deque<Job> readyJobs;
    private int failureCount;
    private String messageId;


    NotificationSubscriberThread(String topic, @Nullable String messageId) {
      super(String.format("NotificationSubscriberThread-%s", topic));
      this.topic = topic;
      this.messageId = messageId;
      // TODO: [CDAP-11370] Need to be configured in cdap-default.xml. Retry with delay ranging from 0.1s to 30s
      scheduleStrategy =
        co.cask.cdap.common.service.RetryStrategies.exponentialDelay(100, 30000, TimeUnit.MILLISECONDS);
      this.readyJobs = new ArrayDeque<>();
    }

    void addJob(Job job) {
      readyJobs.add(job);
    }

    @Override
    public void run() {
      while (isRunning()) {
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
     * Fetch new notifications and update job queue
     *
     * @return sleep time in milliseconds before next fetch
     */
    private long processNotifications() {
      boolean emptyFetch = false;
      try {
        final MessageFetcher fetcher = messagingContext.getMessageFetcher();
        emptyFetch = Transactions.execute(transactional, new TxCallable<Boolean>() {
          @Override
          public Boolean call(DatasetContext context) throws Exception {
            return fetchAndProcessNotifications(context, fetcher);
          }
        });
        failureCount = 0;
      } catch (Exception e) {
        LOG.warn("Failed to get notification. Will retry in next run", e);
        failureCount++;
      }

      // Still need to run jobs if the queue is not empty to avoid unlimited growth on the job queue
      // This can happen if it fetches some notification from TMS
      runReadyJobs();

      // If there is any failure during fetching of notifications or looking up of schedules,
      // delay the next fetch based on the strategy
      if (failureCount > 0) {
        // Exponential strategy doesn't use the time component, so doesn't matter what we passed in as startTime
        return scheduleStrategy.nextRetry(failureCount, 0);
      }

      // Sleep for 2 seconds if there's no notification, otherwise don't sleep
      return emptyFetch ? 2000L : 0L;
    }

    private boolean fetchAndProcessNotifications(DatasetContext context, MessageFetcher fetcher) throws Exception {
      boolean emptyFetch = true;
      try (CloseableIterator<Message> iterator = fetcher.fetch(NamespaceId.SYSTEM.getNamespace(),
                                                               topic, 100, messageId)) {
        LOG.trace("Fetch with messageId = {}", messageId);
        while (iterator.hasNext() && isRunning()) {
          emptyFetch = false;
          Message message = iterator.next();
          Notification notification;
          try {
            notification = GSON.fromJson(new String(message.getPayload(), StandardCharsets.UTF_8),
                                         Notification.class);
          } catch (JsonSyntaxException e) {
            LOG.warn("Failed to decode message with id {}. Skipped. ", message.getId(), e);
            messageId = message.getId(); // update messageId to skip this message in next fetch
            continue;
          }
          updateJobQueue(context, notification);
          messageId = message.getId();
        }
      } catch (ServiceUnavailableException | TopicNotFoundException e) {
        SAMPLING_LOG.info("Failed to fetch from TMS. Will retry later.", e);
        failureCount++;
      }
      return emptyFetch;
    }

    private void runReadyJobs() {
      Iterator<Job> jobIterator = readyJobs.iterator();
      while (jobIterator.hasNext()) {
        Job job = jobIterator.next();
        ProgramSchedule schedule = job.getSchedule();
        try {
          // TODO: Temporarily execute scheduled program without any checks. Need to check appSpec and scheduleSpec
          taskRunner.execute(schedule.getProgramId(), ImmutableMap.<String, String>of(),
                             ImmutableMap.<String, String>of());
          LOG.debug("Run program {} in schedule", schedule.getProgramId(), schedule.getName());
        } catch (Exception e) {
          LOG.warn("Failed to run program {} in schedule {}. Skip running this program.",
                   schedule.getProgramId(), schedule.getName(), e);
        }
        jobIterator.remove();
      }
    }

    abstract void updateJobQueue(DatasetContext context, Notification notification) throws Exception;
  }

  private class TimeEventNotificationSubscriberThread extends NotificationSubscriberThread {

    TimeEventNotificationSubscriberThread(@Nullable String messageId) {
      super(cConf.get(Constants.Scheduler.TIME_EVENT_TOPIC), messageId);
    }

    @Override
    protected void updateJobQueue(DatasetContext context, Notification notification) {

    }
  }

  private class DataEventNotificationSubscriberThread extends NotificationSubscriberThread {

    DataEventNotificationSubscriberThread(@Nullable String messageId) {
      super(cConf.get(Constants.Dataset.DATA_EVENT_TOPIC), messageId);
    }

    @Override
    protected void updateJobQueue(DatasetContext context, Notification notification)
      throws IOException, DatasetManagementException {
      String datasetIdString = notification.getProperties().get("datasetId");
      if (datasetIdString == null) {
        return;
      }
      DatasetId datasetId = DatasetId.fromString(datasetIdString);
      for (ProgramSchedule schedule : getSchedules(context, Schedulers.triggerKeyForPartition(datasetId))) {
        addJob(new Job(schedule));
      }
    }
  }

  private static final class Job {
    ProgramSchedule schedule;

    Job(ProgramSchedule schedule) {
      this.schedule = schedule;
    }

    public ProgramSchedule getSchedule() {
      return schedule;
    }
  }

  private Collection<ProgramSchedule> getSchedules(DatasetContext context, String triggerKey)
    throws IOException, DatasetManagementException {
    return getScheduleDataset(context).findSchedules(triggerKey);
  }

  private ProgramScheduleStoreDataset getScheduleDataset(DatasetContext context)
    throws IOException, DatasetManagementException {
    return DatasetsUtil.getOrCreateDataset(context, datasetFramework, Schedulers.STORE_DATASET_ID,
                                           Schedulers.STORE_TYPE_NAME, DatasetProperties.EMPTY);
  }
}
