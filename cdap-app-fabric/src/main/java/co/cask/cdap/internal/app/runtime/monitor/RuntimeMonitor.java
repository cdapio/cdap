/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.monitor;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.messaging.MessagePublisher;
import co.cask.cdap.api.messaging.MessagingContext;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.common.service.AbstractRetryableScheduledService;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.store.AppMetadataStore;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Runtime Monitor Service responsible for fetching program status, audit messages, metrics, data events and metadata
 */
public class RuntimeMonitor extends AbstractRetryableScheduledService {

  private static final Logger LOG = LoggerFactory.getLogger(RuntimeMonitor.class);
  // For outage, only log once per 60 seconds per message.
  private static final Logger OUTAGE_LOG = Loggers.sampling(LOG, LogSamplers.perMessage(
    () -> LogSamplers.limitRate(60000)));

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
  private final DatasetFramework datasetFramework;
  private final Transactional transactional;
  private final MessagingContext messagingContext;
  private final ScheduledExecutorService scheduledExecutorService;

  private Map<String, MonitorConsumeRequest> topicsToRequest;
  private long programFinishTime;

  public RuntimeMonitor(ProgramRunId programRunId, CConfiguration cConf, RuntimeMonitorClient monitorClient,
                        DatasetFramework datasetFramework, Transactional transactional,
                        MessagingContext messagingContext, ScheduledExecutorService scheduledExecutorService) {
    super(RetryStrategies.fromConfiguration(cConf, "system.runtime.monitor."));

    this.programRunId = programRunId;
    this.cConf = cConf;
    this.monitorClient = monitorClient;
    this.limit = cConf.getInt(Constants.RuntimeMonitor.BATCH_LIMIT);
    this.pollTimeMillis = cConf.getLong(Constants.RuntimeMonitor.POLL_TIME_MS);
    this.gracefulShutdownMillis = cConf.getLong(Constants.RuntimeMonitor.GRACEFUL_SHUTDOWN_MS);
    this.topicsToRequest = new HashMap<>();
    this.datasetFramework = datasetFramework;
    this.messagingContext = messagingContext;
    this.transactional = transactional;
    this.scheduledExecutorService = scheduledExecutorService;
    this.programFinishTime = -1L;
    this.lastProgramStateMessages = new LinkedList<>();
    this.requestKeyToLocalTopic = createTopicConfigs(cConf);
  }

  @Override
  protected ScheduledExecutorService executor() {
    return scheduledExecutorService;
  }

  @Override
  protected void doShutdown() {
    if (!lastProgramStateMessages.isEmpty()) {
      String topicConfig = Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC;
      String topic = requestKeyToLocalTopic.get(topicConfig);

      // Publish last program state messages with Retries
      Retries.runWithRetries(() -> Transactionals.execute(transactional, context -> {
        AppMetadataStore store = AppMetadataStore.create(cConf, context, datasetFramework);
        publish(topicConfig, topic, lastProgramStateMessages, store);

        // cleanup all the offsets after publishing terminal states.
        for (String topicConf : requestKeyToLocalTopic.keySet()) {
          store.deleteSubscriberState(topicConf, programRunId.getRun());
        }
      }), getRetryStrategy());
    }
  }

  /**
   * Kills the runtime.
   */
  public void kill() throws Exception {
    // Just issue a kill command to the runtime monitor server.
    // If the program is running, it will publish a killed state
    monitorClient.kill();
  }

  @Override
  protected boolean shouldRetry(Exception ex) {
    OUTAGE_LOG.warn("Failed to fetch monitoring data from program run {}. Will be retried in next iteration.",
                    programRunId, ex);
    return true;
  }

  @Override
  protected String getServiceName() {
    return "runtime-monitor-" + programRunId.getRun();
  }

  @Override
  protected long runTask() throws Exception {
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
    long latestPublishTime = Transactionals.execute(transactional, context -> {
      AppMetadataStore store = AppMetadataStore.create(cConf, context, datasetFramework);
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

    Transactionals.execute(transactional, context -> {
      AppMetadataStore store = AppMetadataStore.create(cConf, context, datasetFramework);

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
    MessagePublisher messagePublisher = messagingContext.getMessagePublisher();
    messagePublisher.publish(NamespaceId.SYSTEM.getNamespace(), topic,
                             messages.stream().map(MonitorMessage::getMessage).iterator());

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
    try {
      monitorClient.requestShutdown();
    } catch (ServiceUnavailableException e) {
      LOG.trace("Runtime monitor server is unavailable on shutting down of program run {}", programRunId);
    } catch (Exception e) {
      LOG.warn("Exception raised when attempting to shutdown runtime for program run {}", programRunId);
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

      if (programStatus.equals(ProgramRunStatus.COMPLETED.name()) ||
        programStatus.equals(ProgramRunStatus.FAILED.name()) ||
        programStatus.equals(ProgramRunStatus.KILLED.name())) {
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
