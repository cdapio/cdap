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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.messaging.MessagePublisher;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.context.MultiThreadMessagingContext;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.ServiceUnavailableException;

/**
 * Runtime Monitor Runnable responsible for fetching metadata
 */
public class RuntimeMonitor extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(RuntimeMonitor.class);
  // For outage, only log once per 60 seconds per message.
  private static final Logger OUTAGE_LOG =  Loggers.sampling(LOG, LogSamplers.perMessage(
    () -> LogSamplers.limitRate(60000)));

  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_MESSAGE_TYPE = new TypeToken<Map<String, List<MonitorMessage>>>() { }.getType();

  private final RESTClient restClient;
  private final ClientConfig clientConfig;

  private final int limit;
  private final ProgramRunId programRunId;
  private final CConfiguration cConf;
  private final Map<String, MonitorConsumeRequest> topicsToRequest;
  // caches request key to topic
  private final Map<String, String> requestKeyToLocalTopic;

  private final MessagePublisher messagePublisher;
  private final long pollTimeMillis;
  private final long gracefulShutdownMillis;
  private final List<MonitorMessage> lastProgramStateMessages;
  private volatile Thread runThread;
  private volatile boolean stopped;
  private long programFinishTime;

  public RuntimeMonitor(ProgramRunId programRunId, CConfiguration cConf,
                        MessagingService messagingService, ClientConfig clientConfig) {
    this.programRunId = programRunId;
    this.cConf = cConf;
    this.messagePublisher = new MultiThreadMessagingContext(messagingService).getMessagePublisher();
    this.clientConfig = clientConfig;
    this.restClient = new RESTClient(clientConfig);
    this.limit = cConf.getInt(Constants.RuntimeMonitor.BATCH_LIMIT);
    this.pollTimeMillis = cConf.getLong(Constants.RuntimeMonitor.POLL_TIME_MS);
    this.gracefulShutdownMillis = cConf.getLong(Constants.RuntimeMonitor.GRACEFUL_SHUTDOWN_MS);
    this.topicsToRequest = new HashMap<>();
    this.requestKeyToLocalTopic = new HashMap<>();
    this.programFinishTime = -1L;
    this.lastProgramStateMessages = new ArrayList<>();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.debug("Starting runtime monitor for program run {}.", programRunId);
    // TODO initialize from offset table for a given programId
    for (String topicConfig : getTopicConfigs()) {
      topicsToRequest.put(topicConfig, new MonitorConsumeRequest(null, limit));
      requestKeyToLocalTopic.put(topicConfig, getTopic(topicConfig));
    }
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.debug("Shutting down runtime monitor for program run {}.", programRunId);
  }

  @Override
  protected void triggerShutdown() {
    stopped = true;
    if (runThread != null) {
      runThread.interrupt();
    }
  }

  private Set<String> getTopicConfigs() {
    Set<String> topicsConfigsToMonitor = new HashSet<>();

    for (String topicConfig : cConf.getTrimmedStringCollection(Constants.RuntimeMonitor.TOPICS_CONFIGS)) {
      int idx = topicConfig.lastIndexOf(':');

      if (idx < 0) {
        topicsConfigsToMonitor.add(topicConfig);
        continue;
      }

      try {
        int totalTopicCount = Integer.parseInt(topicConfig.substring(idx + 1));
        if (totalTopicCount <= 0) {
          throw new IllegalArgumentException("Total topic number must be positive for system topic config '" +
                                               topicConfig + "'.");
        }

        String config = topicConfig.substring(0, idx);
        for (int i = 0; i < totalTopicCount; i++) {
          // For metrics, We make an assumption that number of metrics topics on runtime are not different than
          // cdap system. So, we will add same number of topic configs as number of metrics topics so that we can
          // keep track of different offsets for each metrics topic.
          // TODO: CDAP-13303 - Handle different number of metrics topics between runtime and cdap system
          topicsConfigsToMonitor.add(config + ":" + i);
        }
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Total topic number must be a positive number for system topic config'"
                                             + topicConfig + "'.", e);
      }
    }

    return topicsConfigsToMonitor;
  }

  @Override
  protected void run() {
    runThread = Thread.currentThread();
    boolean shutdownRemoteRuntime = false;

    try {
      while (!stopped) {
        try {
          // First check if we should trigger the remote service to shutdown and also shutting down this monitor.
          // If the triggerRuntimeShutdown() call failed, we'll stay in this loop for the retry.
          if (shutdownRemoteRuntime) {
            LOG.debug("Program run {} completed at {}, shutting down remote runtime at {}",
                      programRunId, programFinishTime, clientConfig.resolveURL("runtime"));
            triggerRuntimeShutdown();
            break;
          }

          // Next to fetch data from the remote runtime
          HttpResponse response = restClient
            .execute(HttpRequest.builder(HttpMethod.POST, clientConfig.resolveURL("runtime/metadata"))
                       .withBody(GSON.toJson(topicsToRequest)).build());

          if (response.getResponseCode() == HttpURLConnection.HTTP_UNAVAILABLE) {
            throw new ServiceUnavailableException(response.getResponseBodyAsString());
          }

          Map<String, List<MonitorMessage>> monitorResponses =
            GSON.fromJson(response.getResponseBodyAsString(StandardCharsets.UTF_8), MAP_STRING_MESSAGE_TYPE);

          long latestPublishTime = processResponse(monitorResponses);

          // If we got the program finished state, determine when to shutdown
          if (programFinishTime > 0) {
            // Gives half the time of the graceful shutdown time to allow empty fetches
            // Essentially is the wait time for any unpublished events on the remote runtime to publish
            // E.g. Metrics from the remote runtime process might have some delay after the program state changed,
            // even though we explicitly flush the metrics on program completion.
            long now = System.currentTimeMillis();
            if ((latestPublishTime < 0 && now - (gracefulShutdownMillis >> 1) > programFinishTime)
              || (now - gracefulShutdownMillis > programFinishTime)) {

              shutdownRemoteRuntime = true;
              continue;     // Continue to skip the sleep and proceed to the triggerRuntimeShutdown call.
            }
          }

          Thread.sleep(pollTimeMillis);
        } catch (Exception e) {
          if (programFinishTime < 0) {
            // TODO: CDAP-13343 Shouldn't just loop infinitely.
            // Need to check why it cannot connect and decide what to do.
            OUTAGE_LOG.warn("Failed to fetch monitoring data from program run {}. Will be retried in next iteration.",
                            programRunId, e);
          }

          Thread.sleep(pollTimeMillis);
        }
      }
    } catch (InterruptedException e) {
      // Interruption means stopping the service.
    }

    // Clear the interrupt flag
    Thread.interrupted();

    if (stopped) {
      // TODO: Kill the remote program execute on explicit stop on this monitor
    }

    // Publish the remaining program state messages. We should retry indefinitely
    String topicConfig = Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC;
    String topic = requestKeyToLocalTopic.get(topicConfig);
    try {
      Retries.callWithRetries(() -> publish(topicConfig, topic, lastProgramStateMessages),
                              RetryStrategies.fixDelay(1, TimeUnit.SECONDS), Retries.ALWAYS_TRUE);
    } catch (Exception e) {
      // This shouldn't happen since we have infinite retry.
      LOG.error("Failed to publish remaining program state messages for program run {}", programRunId, e);
    }
  }

  /**
   * Processes the given set of monitoring messages.
   *
   * @param monitorResponses set of messages to be processed.
   * @return the latest message publish time or {@code -1L} if there is no message to process
   */
  private long processResponse(Map<String, List<MonitorMessage>> monitorResponses) throws Exception {
    long latestPublishTime = -1L;

    for (Map.Entry<String, List<MonitorMessage>> monitorResponse : monitorResponses.entrySet()) {
      String topicConfig = monitorResponse.getKey();
      List<MonitorMessage> monitorMessages = monitorResponse.getValue();

      if (monitorMessages.isEmpty()) {
        continue;
      }

      if (topicConfig.equals(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC)) {
        if (programFinishTime < 0) {
          programFinishTime = findProgramFinishTime(monitorMessages);
        }

        if (programFinishTime >= 0) {
          // Buffer the program state messages and don't publish them until the end
          // Otherwise, once we publish, the deprovisioner will kicks in and deleting the cluster
          // which could result in losing the last set of messages for some topics.
          lastProgramStateMessages.addAll(monitorMessages);
          // We still update the in memory store for the next fetch offset to avoid fetching duplicate
          // messages, however, that shouldn't be persisted to avoid potential loss of messages
          // in case of failure
          updateTopicToRequest(topicConfig, monitorMessages);
          continue;
        }
      }

      String topic = requestKeyToLocalTopic.get(topicConfig);
      latestPublishTime = Math.max(publish(topicConfig, topic, monitorMessages), latestPublishTime);
    }
    return latestPublishTime;
  }

  /**
   * Publish the given set of messages to the local TMS.
   *
   * @return the latest message publish time or {@code -1L} if there is no message to process
   */
  private long publish(String topicConfig, String topic, List<MonitorMessage> messages) throws Exception {
    // TODO publish messages transactionally along with offset table updates
    messagePublisher.publish(NamespaceId.SYSTEM.getNamespace(), topic,
                             messages.stream().map(s -> s.getMessage().getBytes(StandardCharsets.UTF_8)).iterator());

    MonitorMessage lastMessage = updateTopicToRequest(topicConfig, messages);

    // Messages are ordered by publish time, hence we can get the latest publish time by getting the publish time
    // from the last message.
    return getMessagePublishTime(lastMessage);
  }

  /**
   * Updates the in memory map for the given topic and consume request based on the messages being processed.
   */
  private MonitorMessage updateTopicToRequest(String topicConfig, List<MonitorMessage> messages) {
    MonitorMessage lastMessage = messages.get(messages.size() - 1);
    topicsToRequest.put(topicConfig, new MonitorConsumeRequest(lastMessage.getMessageId(), limit));
    return lastMessage;
  }

  /**
   * Calls into the remote runtime to ask for it to terminate itself.
   */
  private void triggerRuntimeShutdown() throws Exception {
    try {
      restClient.execute(HttpRequest.builder(HttpMethod.POST, clientConfig.resolveURL("runtime/shutdown")).build());
    } catch (ConnectException e) {
      LOG.trace("Connection refused when attempting to connect to Runtime Http Server. " +
                  "Assuming that it is not available.");
    }
  }

  /**
   * Returns the time where the program finished, meaning it reaches one of the terminal states. If the given
   * list of {@link MonitorMessage} doesn't contain such information, {@code -1L} is returned.
   */
  private long findProgramFinishTime(List<MonitorMessage> monitorMessages) {
    for (MonitorMessage message : monitorMessages) {
      Notification notification = GSON.fromJson(message.getMessage(), Notification.class);
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

  private String getTopic(String topicConfig) {
    int idx = topicConfig.lastIndexOf(':');
    return idx < 0 ? cConf.get(topicConfig) : cConf.get(topicConfig.substring(0, idx)) + topicConfig.substring(idx + 1);
  }
}
