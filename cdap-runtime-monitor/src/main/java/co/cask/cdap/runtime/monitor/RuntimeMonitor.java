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

package co.cask.cdap.runtime.monitor;

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Runtime Monitor Runnable responsible for fetching metadata
 */
public class RuntimeMonitor extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(RuntimeMonitor.class);
  private static final Gson GSON = new Gson();
  private final RESTClient restClient;
  private final ClientConfig clientConfig;
  private final int limit;
  private String[] topics;
  private String runtimeId;
  private long pollTimeMs;
  private volatile Thread runThread;
  private volatile boolean stopped;

  // TODO use persistent storage for offset management
  private Map<String, Integer> map = new HashMap<>();

  public RuntimeMonitor(String runtimeId, int pollTimeMs, ClientConfig clientConfig, int limit, String... topics) {
    this.clientConfig = clientConfig;
    this.restClient = new RESTClient(clientConfig);
    this.limit = limit;
    this.topics = topics;
    this.pollTimeMs = pollTimeMs;
  }

  @Override
  protected void startUp() throws Exception {
    // TODO initialize from offset table
    LOG.debug("Starting runtime monitor {}....", runtimeId);
    initialize(topics);
  }

  private void initialize(String... topics) {
    for (String topic : topics) {
      map.put(topic, 0);
    }
  }

  private List<MonitorTopic> initializeList() {
    List<MonitorTopic> topics = new ArrayList<>();
    for (Map.Entry<String, Integer> topic : map.entrySet()) {
      topics.add(new MonitorTopic(topic.getKey(), topic.getValue(), limit));
    }
    return topics;
  }

  @Override
  protected void run() throws Exception {
    runThread = Thread.currentThread();

    try {
      // TODO - initialize with persisted offsets
      List<MonitorTopic> topics = initializeList();

      while (!stopped) {
        HttpResponse response = restClient.execute(HttpMethod.GET, clientConfig.getConnectionConfig().resolveURI
                                                     (clientConfig.getApiVersion() + "/runtime/metadata").toURL(),
                                                   GSON.toJson(topics), ImmutableMap.<String, String>of(),
                                                   clientConfig.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND,
                                                   HttpURLConnection.HTTP_BAD_REQUEST);

        if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
          throw new BadRequestException(response.getResponseBodyAsString());
        }
        if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
          throw new NotFoundException(response.getResponseBodyAsString());
        }

        // TODO deserialize and persist on table
        LOG.debug(response.getResponseBodyAsString());
        Thread.sleep(pollTimeMs);
      }
    }catch (InterruptedException e) {
      // Interruption means stopping the service.
    }
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.debug("Shutting down runtime monitor {}....", runtimeId);
  }

  @Override
  protected void triggerShutdown() {
    stopped = true;
    if (runThread != null) {
      runThread.interrupt();
    }
  }
}
