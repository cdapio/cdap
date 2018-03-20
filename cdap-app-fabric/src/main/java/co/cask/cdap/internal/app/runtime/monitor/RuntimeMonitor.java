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
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
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
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type MAP_STRING_MESSAGE_TYPE = new TypeToken<Map<String, List<MonitorMessage>>>() { }.getType();

  private final RESTClient restClient;
  private final ClientConfig clientConfig;

  private final int limit;
  private final ProgramRunId programId;
  private final CConfiguration cConf;
  private final Map<String, MonitorConsumeRequest> topicsToRequest;

  private final MessagePublisher messagePublisher;
  private final long pollTimeMillis;
  private volatile Thread runThread;
  private volatile boolean stopped;

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
  }

  @Override
  protected void startUp() throws Exception {
    LOG.debug("Starting runtime monitor for program {} and run {}.", programId.getProgram(), programId.getRun());
  }

  private void initializeTopics() throws Exception {
    Set<String> topicsToMonitor = new HashSet<>();
    topicsToMonitor.addAll(Arrays.asList(cConf.get(Constants.RuntimeMonitor.TOPICS).split(",")));

    HttpResponse response = restClient.execute(HttpMethod.POST, clientConfig.resolveURL("runtime/monitor/topics"),
                                               GSON.toJson(topicsToMonitor), Collections.emptyMap(), null);

    if (response.getResponseCode() == HttpURLConnection.HTTP_UNAVAILABLE) {
      throw new ServiceUnavailableException(response.getResponseBodyAsString());
    }

    Map<String, String> configToTopic = GSON.fromJson(response.getResponseBodyAsString(StandardCharsets.UTF_8),
                                                      MAP_STRING_STRING_TYPE);

    // TODO initialize from offset table for a given programId
    for (Map.Entry<String, String> entry : configToTopic.entrySet()) {
      topicsToRequest.put(entry.getKey(), new MonitorConsumeRequest(entry.getValue(), entry.getKey(), null, limit));
    }
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

          processResponse(monitorResponses);
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

  private void processResponse(Map<String, List<MonitorMessage>> monitorResponses) throws Exception {
    for (Map.Entry<String, MonitorConsumeRequest> request : topicsToRequest.entrySet()) {
      publish(request.getKey(), request.getValue(), monitorResponses.get(request.getValue().getTopic()));
    }
  }

  private void publish(String topicConfig, MonitorConsumeRequest request,
                       List<MonitorMessage> messages) throws Exception {
    if (messages.isEmpty()) {
      return;
    }

    // TODO publish messages transactionally along with offset table updates
    messagePublisher.publish(NamespaceId.SYSTEM.getNamespace(), cConf.get(topicConfig),
                             messages.stream().map(s -> s.getMessage().getBytes(StandardCharsets.UTF_8)).iterator());

    topicsToRequest.put(topicConfig, new MonitorConsumeRequest(request.getTopic(), topicConfig,
                                                               messages.get(messages.size() - 1).getMessageId(),
                                                               limit));
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
