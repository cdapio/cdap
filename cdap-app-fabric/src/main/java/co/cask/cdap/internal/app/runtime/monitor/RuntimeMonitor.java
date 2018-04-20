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

import co.cask.cdap.api.messaging.MessagePublisher;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private final ProgramRunId programId;
  private final CConfiguration cConf;
  private final Map<String, MonitorConsumeRequest> topicsToRequest;
  // caches request key to topic
  private final Map<String, String> requestKeyToLocalTopic;

  private final MessagePublisher messagePublisher;
  private final long pollTimeMillis;
  private volatile Thread runThread;
  private volatile boolean stopped;
  private boolean programFinished;

  public RuntimeMonitor(ProgramRunId programId, CConfiguration cConf, MessagePublisher messagePublisher,
                        ClientConfig clientConfig) {
    this.programId = programId;
    this.cConf = cConf;
    this.messagePublisher = messagePublisher;
    this.clientConfig = clientConfig;
    this.restClient = new RESTClient(clientConfig);
    this.limit = cConf.getInt(Constants.RuntimeMonitor.BATCH_LIMIT);
    this.pollTimeMillis = cConf.getInt(Constants.RuntimeMonitor.POLL_TIME_MS);
    this.topicsToRequest = new HashMap<>();
    this.requestKeyToLocalTopic = new HashMap<>();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.debug("Starting runtime monitor for program {} and run {}.", programId.getProgram(), programId.getRun());
  }

  private void initializeTopics() throws Exception {
    // TODO initialize from offset table for a given programId
    for (String topicConfig : getTopicConfigs()) {
      topicsToRequest.put(topicConfig, new MonitorConsumeRequest(null, limit));
      requestKeyToLocalTopic.put(topicConfig, getTopic(topicConfig));
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

    try {
      while (!stopped) {
        try {
          if (topicsToRequest.isEmpty()) {
            initializeTopics();
          }

          HttpResponse response = restClient
            .execute(HttpRequest.builder(HttpMethod.POST, clientConfig.resolveURL("runtime/metadata"))
                       .withBody(GSON.toJson(topicsToRequest)).build());

          if (response.getResponseCode() == HttpURLConnection.HTTP_UNAVAILABLE) {
            throw new ServiceUnavailableException(response.getResponseBodyAsString());
          }

          Map<String, List<MonitorMessage>> monitorResponses =
            GSON.fromJson(response.getResponseBodyAsString(StandardCharsets.UTF_8), MAP_STRING_MESSAGE_TYPE);

          if (processResponse(monitorResponses) == 0 && programFinished) {
            triggerRuntimeShutdown();
          }
        } catch (Exception e) {
          OUTAGE_LOG.warn("Failed to fetch monitoring data from program {}, run {}. Will be retried in next iteration.",
                          programId.getProgram(), programId.getRun(), e);
        }
        Thread.sleep(pollTimeMillis);
      }
    } catch (InterruptedException e) {
      // Interruption means stopping the service.
    }
  }

  private int processResponse(Map<String, List<MonitorMessage>> monitorResponses) throws Exception {
    int count = 0;
    for (Map.Entry<String, List<MonitorMessage>> monitorResponse : monitorResponses.entrySet()) {
      if (monitorResponse.getKey().equals(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC)) {
        setIsRuntimeInactive(monitorResponse.getValue());
      }

      String topic = requestKeyToLocalTopic.get(monitorResponse.getKey());
      publish(monitorResponse.getKey(), topic, monitorResponse.getValue());
      count += monitorResponse.getValue().size();
    }
    return count;
  }

  private String getTopic(String topicConfig) {
    int idx = topicConfig.lastIndexOf(':');
    return idx < 0 ? cConf.get(topicConfig) : cConf.get(topicConfig.substring(0, idx)) + topicConfig.substring(idx + 1);
  }

  private void publish(String topicConfig, String topic, List<MonitorMessage> messages) throws Exception {
    if (messages.isEmpty()) {
      return;
    }

    // TODO publish messages transactionally along with offset table updates
    messagePublisher.publish(NamespaceId.SYSTEM.getNamespace(), topic,
                             messages.stream().map(s -> s.getMessage().getBytes(StandardCharsets.UTF_8)).iterator());

    topicsToRequest.put(topicConfig, new MonitorConsumeRequest(messages.get(messages.size() - 1).getMessageId(),
                                                               limit));
  }

  private void triggerRuntimeShutdown() throws Exception {
    try {
      restClient.execute(HttpRequest.builder(HttpMethod.POST, clientConfig.resolveURL("runtime/shutdown")).build());
    } catch (ConnectException e) {
      LOG.trace("Connection refused when attempting to connect to Runtime Http Server. " +
                  "Assuming that it is not available.");
    }
  }

  private void setIsRuntimeInactive(List<MonitorMessage> monitorMessages) {
    for (MonitorMessage message : monitorMessages) {
      Notification notification = GSON.fromJson(message.getMessage(), Notification.class);
      String programStatus = notification.getProperties().get(ProgramOptionConstants.PROGRAM_STATUS);
      if (programStatus.equals(ProgramRunStatus.COMPLETED.name()) ||
        programStatus.equals(ProgramRunStatus.FAILED.name()) ||
        programStatus.equals(ProgramRunStatus.KILLED.name())) {
        programFinished = true;
        break;
      }
    }
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.debug("Shutting down runtime monitor for program {} and run {}.", programId.getProgram(), programId.getRun());
  }

  @Override
  protected void triggerShutdown() {
    stopped = true;
    if (runThread != null) {
      runThread.interrupt();
    }
  }
}
