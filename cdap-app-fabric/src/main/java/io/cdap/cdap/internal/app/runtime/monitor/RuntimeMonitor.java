/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.messaging.MessagePublisher;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.logging.LogSamplers;
import io.cdap.cdap.common.logging.Loggers;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.service.AbstractRetryableScheduledService;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.distributed.remote.RemoteProcessController;
import io.cdap.cdap.internal.app.runtime.distributed.remote.RemoteRuntimeTable;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Runtime Monitor Service responsible for fetching program status, audit messages, metrics, data events and metadata
 */
public class RuntimeMonitor extends AbstractRetryableScheduledService {

  private static final Logger LOG = LoggerFactory.getLogger(RuntimeMonitor.class);
  // Skip the first error log, and at most log once per 30 seconds.
  // This helps debugging errors that persist more than 30 seconds.
  private static final Logger OUTAGE_LOGGER = Loggers.sampling(
    LOG, LogSamplers.all(LogSamplers.skipFirstN(1), LogSamplers.limitRate(TimeUnit.SECONDS.toMillis(30))));


  private static final Gson GSON = new Gson();

  private final RuntimeMonitorClient monitorClient;

  private final int limit;
  private final ProgramRunId programRunId;
  private final CConfiguration cConf;
  // caches request key to topic
  private final Map<String, String> requestKeyToLocalTopic;

  private final long pollTimeMillis;
  private final long gracefulShutdownMillis;
  private final Deque<MonitorMessage> lastProgramStateMessages;
  private final MessagingContext messagingContext;
  private final ScheduledExecutorService scheduledExecutorService;
  private final RemoteExecutionLogProcessor logProcessor;
  private final RemoteProcessController remoteProcessController;
  private final ProgramStateWriter programStateWriter;
  private final TransactionRunner transactionRunner;
  private final List<Service> extraServices;

  private Map<String, MonitorConsumeRequest> topicsToRequest;
  private long programFinishTime;

  public RuntimeMonitor(ProgramRunId programRunId, CConfiguration cConf, RuntimeMonitorClient monitorClient,
                        MessagingContext messagingContext, ScheduledExecutorService scheduledExecutorService,
                        RemoteExecutionLogProcessor logProcessor, RemoteProcessController remoteProcessController,
                        ProgramStateWriter programStateWriter, TransactionRunner transactionRunner,
                        Service... extraServices) {
    super(RetryStrategies.fromConfiguration(cConf, "system.runtime.monitor."));

    this.programRunId = programRunId;
    this.cConf = cConf;
    this.monitorClient = monitorClient;
    this.limit = cConf.getInt(Constants.RuntimeMonitor.BATCH_SIZE);
    this.pollTimeMillis = cConf.getLong(Constants.RuntimeMonitor.POLL_TIME_MS);
    this.gracefulShutdownMillis = cConf.getLong(Constants.RuntimeMonitor.GRACEFUL_SHUTDOWN_MS);
    this.topicsToRequest = new HashMap<>();
    this.messagingContext = messagingContext;
    this.scheduledExecutorService = scheduledExecutorService;
    this.logProcessor = logProcessor;
    this.programFinishTime = -1L;
    this.lastProgramStateMessages = new LinkedList<>();
    this.requestKeyToLocalTopic = createTopicConfigs(cConf);
    this.remoteProcessController = remoteProcessController;
    this.programStateWriter = programStateWriter;
    this.transactionRunner = transactionRunner;
    this.extraServices = new ArrayList<>(Arrays.asList(extraServices));
  }

  @Override
  protected ScheduledExecutorService executor() {
    return scheduledExecutorService;
  }

  @Override
  protected void doStartUp() {
    LOG.debug("Start monitoring program run {}", programRunId);
    for (Service service : extraServices) {
      try {
        service.startAndWait();
      } catch (Exception e) {
        LOG.warn("Failed to start service {} for program run {}", service, programRunId);
      }
    }
  }

  @Override
  protected void doShutdown() {
    for (Service service : Lists.reverse(extraServices)) {
      try {
        service.stopAndWait();
      } catch (Exception e) {
        LOG.warn("Failed to stop service {} for program run {}", service, programRunId);
      }
    }
    LOG.debug("Stopped monitoring program run {}", programRunId);
  }

  /**
   * Kills the runtime.
   */
  public void requestStop() throws Exception {
    // Just issue a kill command to the runtime monitor server.
    // If the program is running, it will publish a killed state
    monitorClient.requestStop();
  }

  @Override
  protected boolean shouldRetry(Exception ex) {
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContextWithRunId(programRunId, null);
    Cancellable cancellable = LoggingContextAccessor.setLoggingContext(loggingContext);
    try {
      OUTAGE_LOGGER.warn("Failed to fetch monitoring messages for program {}", programRunId, ex);
      try {
        // If the program is not running, emit error state for the program run and stop the retry
        if (!remoteProcessController.isRunning()) {
          LOG.error("Program runtime terminated abnormally for program {}", programRunId, ex);
          programStateWriter.error(programRunId,
                                   new IllegalStateException("Program runtime terminated abnormally. " +
                                                               "Please inspect logs for root cause.", ex));
          // Clear all program state after erroring out.
          clearStates();
          return false;
        }
      } catch (Exception e) {
        OUTAGE_LOGGER.warn("Failed to check if the remote process is still running for program {}", programRunId, e);
      }
      return true;
    } finally {
      cancellable.cancel();
    }
  }

  @Override
  protected long handleRetriesExhausted(Exception e) throws Exception {
    // kill the remote process and record a program run error
    // log this in the program context so it shows up in program logs
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContextWithRunId(programRunId, null);
    Cancellable cancellable = LoggingContextAccessor.setLoggingContext(loggingContext);
    try {
      LOG.error("Failed to monitor the remote process and exhausted retries. Terminating the program {}",
                programRunId, e);
      try {
        remoteProcessController.kill();
      } catch (Exception e1) {
        LOG.warn("Failed to kill the remote process controller for program {}. "
                   + "The remote process may need to be killed manually.", programRunId, e1);
      }

      // If failed to fetch messages and the remote process is not running, emit a failure program state and
      // terminates the monitor
      programStateWriter.error(programRunId,
                               new IllegalStateException("Program runtime terminated due to too many failures. " +
                                                           "Please inspect logs for root cause.", e));
      // Clear all program state
      clearStates();
      throw e;
    } finally {
      cancellable.cancel();
    }
  }

  @Override
  protected String getServiceName() {
    return "runtime-monitor-" + programRunId.getRun();
  }

  @Override
  protected long runTask() throws Exception {
    LOG.trace("Monitoring remote runtime {}", programRunId);

    // Initialize the topic requests.
    if (topicsToRequest.isEmpty()) {
      topicsToRequest = initTopics(requestKeyToLocalTopic.keySet());
    }

    // Next to fetch data from the remote runtime
    Map<String, Deque<MonitorMessage>> monitorResponses = monitorClient.fetchMessages(topicsToRequest);

    // Update programFinishTime when remote runtime is in terminal state. Also buffer all the program status
    // events. This is done before transactional publishing to avoid re-fetching same remote runtime status
    // messages.
    updateProgramFinishTime(monitorResponses);

    // Publish messages for all the topics and persist corresponding offsets in a single transaction.
    long latestPublishTime = TransactionRunners.run(transactionRunner, context->  {
      AppMetadataStore store = AppMetadataStore.create(context);
      return processResponse(monitorResponses, store);
    });

    // Update in-memory offsets in the topicsToRequest for next iteration
    monitorResponses.forEach(this::updateTopicToRequest);

    // If we got the program finished state, determine when to shutdown
    if (programFinishTime > 0) {
      // Gives half the time of the graceful shutdown time to allow empty fetches
      // Essentially is the wait time for any unpublished events on the remote runtime to publish
      // E.g. Metrics from the remote runtime process might have some delay after the program state changed,
      // even though we explicitly flush the metrics on program completion.
      long now = System.currentTimeMillis();
      if ((latestPublishTime < 0 && now - (gracefulShutdownMillis >> 1) > programFinishTime)
        || (now - gracefulShutdownMillis > programFinishTime)) {
        triggerRuntimeShutdown();
        // Clear all program state after program completed
        clearStates();
        stop();
      }
    }

    return pollTimeMillis;
  }

  /**
   * Detects if the remote runtime is in terminal state. If it is in terminal state, buffers all the program state 
   * messages and updates the in memory store for next fetch
   * 
   * @param monitorResponses map of topic config to queue of monitor messages
   */
  private void updateProgramFinishTime(Map<String, Deque<MonitorMessage>> monitorResponses) {
    if (!monitorResponses.containsKey(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC)) {
      return;
    }

    Deque<MonitorMessage> monitorMessages = monitorResponses.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC);

    if (programFinishTime < 0) {
      programFinishTime = findProgramFinishTime(monitorMessages);
    }

    if (programFinishTime > 0) {
      // Buffer the program state messages and don't publish them until the end
      // Otherwise, once we publish, the deprovisioner will kick in and delete the cluster
      // which could result in losing the last set of messages for some topics.
      lastProgramStateMessages.addAll(monitorMessages);
      // We still update the in memory store for the next fetch offset to avoid fetching duplicate
      // messages, however, that shouldn't be persisted to avoid potential loss of messages
      // in case of failure
      updateTopicToRequest(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC, monitorMessages);
      monitorResponses.remove(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC);
    }
  }

  /**
   * Initialize the {@link MonitorConsumeRequest} for each topic configured by reading the subscriber state for
   * each of the topic.
   *
   * @return a {@link Map} from topic config name to {@link MonitorConsumeRequest}
   * @throws RuntimeException if fetching of the subscriber states failed.
   */
  private Map<String, MonitorConsumeRequest> initTopics(Collection<String> topicConfigs) {
    Map<String, MonitorConsumeRequest> consumeRequests = new HashMap<>();

    TransactionRunners.run(transactionRunner, context->  {
      AppMetadataStore store = AppMetadataStore.create(context);

      for (String topicConfig : topicConfigs) {
        String messageId = store.retrieveSubscriberState(topicConfig, programRunId.getRun());
        consumeRequests.put(topicConfig, new MonitorConsumeRequest(messageId, limit));
      }
    });

    return consumeRequests;
  }

  /**
   * Processes the given set of monitoring messages.
   *
   * @param monitorResponses set of messages to be processed.
   * @return the latest message publish time or {@code -1L} if there is no message to process
   */
  private long processResponse(Map<String, Deque<MonitorMessage>> monitorResponses,
                               AppMetadataStore store) throws Exception {
    long latestPublishTime = -1L;

    for (Map.Entry<String, Deque<MonitorMessage>> monitorResponse : monitorResponses.entrySet()) {
      String topicConfig = monitorResponse.getKey();
      Deque<MonitorMessage> monitorMessages = monitorResponse.getValue();

      if (monitorMessages.isEmpty()) {
        continue;
      }

      long publishTime = publish(topicConfig, requestKeyToLocalTopic.get(topicConfig), monitorMessages, store);
      latestPublishTime = Math.max(publishTime, latestPublishTime);
    }

    return latestPublishTime;
  }

  /**
   * Transactionally publish the given set of messages to the local TMS.
   *
   * @return the latest message publish time
   */
  private long publish(String topicConfig, String topic, Deque<MonitorMessage> messages,
                       AppMetadataStore store) throws Exception {
    // publish messages to tms
    if (topic.startsWith(cConf.get(Constants.Logging.TMS_TOPIC_PREFIX))) {
      logProcessor.process(messages.stream().map(MonitorMessage::getMessage).iterator());
    } else {
      MessagePublisher messagePublisher = messagingContext.getMessagePublisher();
      messagePublisher.publish(NamespaceId.SYSTEM.getNamespace(), topic,
              messages.stream().map(MonitorMessage::getMessage).iterator());
    }

    // persist the last published message as offset in meta store
    MonitorMessage lastMessage = messages.getLast();
    store.persistSubscriberState(topicConfig, programRunId.getRun(), lastMessage.getMessageId());

    // Messages are ordered by publish time, hence we can get the latest publish time by getting the publish time
    // from the last message.
    return getMessagePublishTime(lastMessage);
  }

  /**
   * Updates the in memory map for the given topic and consume request based on the messages being processed.
   */
  private void updateTopicToRequest(String topicConfig, Deque<MonitorMessage> messages) {
    if (messages.isEmpty()) {
      return;
    }

    MonitorMessage lastMessage = messages.getLast();
    topicsToRequest.put(topicConfig, new MonitorConsumeRequest(lastMessage.getMessageId(), limit));
  }

  /**
   * Calls into the remote runtime to ask for it to terminate itself.
   */
  private void triggerRuntimeShutdown() {
    LOG.debug("Program run {} completed at {}, shutting down remote runtime.", programRunId, programFinishTime);

    // Publish all cached program state messages
    if (!lastProgramStateMessages.isEmpty()) {
      String topicConfig = Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC;
      String topic = requestKeyToLocalTopic.get(topicConfig);

      // Publish last program state messages with Retries
      Retries.runWithRetries(() -> TransactionRunners.run(transactionRunner, context->  {
        AppMetadataStore store = AppMetadataStore.create(context);
        publish(topicConfig, topic, lastProgramStateMessages, store);
      }), getRetryStrategy());
    }

    // Request remote runtime to shutdown
    try {
      monitorClient.requestShutdown();
    } catch (Exception e) {
      // If there is exception, use the remote execution controller to try killing the remote process
      try {
        remoteProcessController.kill();
      } catch (Exception ex) {
        LOG.warn("Failed to terminate remote process for program run {}", programRunId, ex);
      }
    }
  }

  /**
   * Clears all the persisted states.
   */
  private void clearStates() {
    try {
      Retries.runWithRetries(() -> TransactionRunners.run(transactionRunner, context->  {
        AppMetadataStore store = AppMetadataStore.create(context);

        // cleanup all the offsets after publishing terminal states.
        for (String topicConf : requestKeyToLocalTopic.keySet()) {
          store.deleteSubscriberState(topicConf, programRunId.getRun());
        }

        RemoteRuntimeTable runtimeDataset = RemoteRuntimeTable.create(context);
        runtimeDataset.delete(programRunId);
      }, RetryableException.class), getRetryStrategy());
    } catch (Exception e) {
      // Just log the exception. The state remained won't affect normal operation.
      LOG.warn("Exception raised when clearing runtime monitor states", e);
    }
  }

  /**
   * Returns the time where the program finished, meaning it reaches one of the terminal states. If the given
   * list of {@link MonitorMessage} doesn't contain such information, {@code -1L} is returned.
   */
  private long findProgramFinishTime(Deque<MonitorMessage> monitorMessages) {
    for (MonitorMessage message : monitorMessages) {
      Notification notification = GSON.fromJson(new String(message.getMessage(), StandardCharsets.UTF_8),
                                                Notification.class);
      if (notification.getNotificationType() != Notification.Type.PROGRAM_STATUS) {
        continue;
      }

      Map<String, String> properties = notification.getProperties();
      String programRun = properties.get(ProgramOptionConstants.PROGRAM_RUN_ID);
      String programStatus = properties.get(ProgramOptionConstants.PROGRAM_STATUS);

      if (programRun == null || programStatus == null) {
        continue;
      }

      // Only match the program state change for the program run it is monitoring
      // For Workflow case, there could be multiple state changes for programs running inside the workflow.
      ProgramRunId programRunId = GSON.fromJson(programRun, ProgramRunId.class);
      if (!programRunId.equals(this.programRunId)) {
        continue;
      }

      if (ProgramRunStatus.isEndState(programStatus)) {
        try {
          return Long.parseLong(properties.get(ProgramOptionConstants.END_TIME));
        } catch (Exception e) {
          // END_TIME should be a valid long. In case there is any problem, use the timestamp in the message ID
          return getMessagePublishTime(message);
        }
      }
    }

    return -1L;
  }

  private long getMessagePublishTime(MonitorMessage message) {
    return new MessageId(Bytes.fromHexString(message.getMessageId())).getPublishTimestamp();
  }

  /**
   * Creates a map from topic configuration name to the actual TMS topic based on the list of topic configuration names
   * specified by the {@link Constants.RuntimeMonitor#TOPICS_CONFIGS} key.
   */
  private static Map<String, String> createTopicConfigs(CConfiguration cConf) {
    return cConf.getTrimmedStringCollection(Constants.RuntimeMonitor.TOPICS_CONFIGS).stream().flatMap(key -> {
      int idx = key.lastIndexOf(':');
      if (idx < 0) {
        return Stream.of(Maps.immutableEntry(key, cConf.get(key)));
      }

      try {
        int totalTopicCount = Integer.parseInt(key.substring(idx + 1));
        if (totalTopicCount <= 0) {
          throw new IllegalArgumentException("Total topic number must be positive for system topic config '" +
                                               key + "'.");
        }
        // For metrics, We make an assumption that number of metrics topics on runtime are not different than
        // cdap system. So, we will add same number of topic configs as number of metrics topics so that we can
        // keep track of different offsets for each metrics topic.
        // TODO: CDAP-13303 - Handle different number of metrics topics between runtime and cdap system
        String topicPrefix = key.substring(0, idx);
        return IntStream
          .range(0, totalTopicCount)
          .mapToObj(i -> Maps.immutableEntry(topicPrefix + ":" + i, cConf.get(topicPrefix) + i));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Total topic number must be a positive number for system topic config'"
                                             + key + "'.", e);
      }
    }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
