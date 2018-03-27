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

package co.cask.cdap.app.runtime.monitor;

import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.MessageFetcher;
import co.cask.cdap.api.messaging.MessagingContext;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.internal.app.runtime.messaging.MultiThreadMessagingContext;
import co.cask.cdap.internal.app.runtime.monitor.MonitorMessage;
import co.cask.cdap.internal.app.runtime.monitor.RuntimeMonitor;
import co.cask.cdap.internal.guice.AppFabricTestModule;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Runtime Monitor Test
 */
@Ignore
public class RuntimeMonitorTest {
  private static final Logger LOG = LoggerFactory.getLogger(RuntimeMonitorTest.class);

  protected static Injector injector;
  protected static CConfiguration cConf;
  protected static MessagingService messagingService;
  private static final Gson GSON = new Gson();
  private HttpServer httpServer;
  private InetSocketAddress address;

  @Before
  public void init() throws IOException, UnsupportedTypeException {
    cConf = CConfiguration.create();
    cConf.set(Constants.AppFabric.OUTPUT_DIR, System.getProperty("java.io.tmpdir"));
    cConf.set(Constants.AppFabric.TEMP_DIR, System.getProperty("java.io.tmpdir"));
    cConf.set(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC, "cdap-status");
    cConf.set(Constants.RuntimeMonitor.BATCH_LIMIT, "2");
    cConf.set(Constants.RuntimeMonitor.POLL_TIME_MS, "20");
    cConf.set(Constants.RuntimeMonitor.TOPICS, Constants.AppFabric.PROGRAM_STATUS_RECORD_EVENT_TOPIC);
    injector = Guice.createInjector(new AppFabricTestModule(cConf));
    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }
    address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 1234);
    httpServer = HttpServer.create(address, 0);
    httpServer.start();
  }

  @After
  public void stop() throws Exception {
    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
    httpServer.stop(0);
  }

  @Test
  public void testRunTimeMonitor() throws Exception {

    Map<String, String> topics = new HashMap<>();
    topics.put(Constants.AppFabric.PROGRAM_STATUS_RECORD_EVENT_TOPIC, "status");

    httpServer.createContext("/v3/runtime/monitor/topics", new HttpHandler() {
      public void handle(HttpExchange exchange) throws IOException {
        byte[] response = GSON.toJson(topics).getBytes();
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
      }
    });

    ConnectionConfig connectionConfig = ConnectionConfig.builder()
      .setHostname(address.getHostName())
      .setPort(1234)
      .setSSLEnabled(false)
      .build();
    ClientConfig.Builder clientConfigBuilder = ClientConfig.builder()
      .setDefaultReadTimeout(20000)
      .setConnectionConfig(connectionConfig);

    int limit = 2;
    MessagingContext messagingContext = new MultiThreadMessagingContext(messagingService);
    RuntimeMonitor runtimeMonitor = new RuntimeMonitor(new ProgramRunId("test", "app1", ProgramType.WORKFLOW, "p1",
                                                                        "run1"),
                                                       cConf, messagingContext.getMessagePublisher(),
                                                       clientConfigBuilder.build());

    Map<String, List<MonitorMessage>> messages = new LinkedHashMap<>();
    ArrayList<MonitorMessage> list = new ArrayList<>();
    list.add(new MonitorMessage("1", "message1"));
    list.add(new MonitorMessage("2", "message2"));
    list.add(new MonitorMessage("3", "message3"));
    list.add(new MonitorMessage("4", "message4"));
    list.add(new MonitorMessage("5", "message5"));
    list.add(new MonitorMessage("6", "message6"));
    list.add(new MonitorMessage("7", "message7"));
    list.add(new MonitorMessage("8", "message8"));
    list.add(new MonitorMessage("9", "message9"));
    list.add(new MonitorMessage("10", "message10"));

    messages.put("status", list);

    httpServer.createContext("/v3/runtime/metadata", new HttpHandler() {
      int count = 0;

      public void handle(HttpExchange exchange) throws IOException {
        Map<String, List<MonitorMessage>> toSend = new LinkedHashMap<>();
        ArrayList<MonitorMessage> list = new ArrayList<>();
        int start = count;
        int i = 0;
        for (MonitorMessage message : messages.get("status")) {
          if (start <= i && i < start + limit) {
            list.add(message);
            count++;
          }
          i++;
        }
        toSend.put("status", list);

        byte[] response = GSON.toJson(toSend).getBytes();
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
      }
    });

    HashSet<String> expected = new LinkedHashSet<>();
    expected.add("message1");
    expected.add("message2");
    expected.add("message3");
    expected.add("message4");
    expected.add("message5");
    expected.add("message6");
    expected.add("message7");
    expected.add("message8");
    expected.add("message9");
    expected.add("message10");

    HashSet<String> actual = new LinkedHashSet<>();
    final String[] messageId = {null};

    runtimeMonitor.startAndWait();

    Tasks.waitFor(
      true,
      new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          MessageFetcher messageFetcher = messagingContext.getMessageFetcher();
          try (CloseableIterator<Message> iter =
                 messageFetcher.fetch(NamespaceId.SYSTEM.getNamespace(),
                                      cConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC), 2, messageId[0])) {
            while (iter.hasNext()) {
              Message message = iter.next();
              messageId[0] = message.getId();
              actual.add(message.getPayloadAsString());
            }
          }

          return expected.size() == actual.size() && expected.equals(actual);
        }
      }, 5, TimeUnit.MINUTES);

    runtimeMonitor.stopAndWait();
  }
}
