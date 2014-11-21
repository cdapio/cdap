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

package co.cask.cdap.gateway.collector;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.stream.StreamEventTypeAdapter;
import co.cask.cdap.gateway.GatewayFastTestsSuite;
import co.cask.cdap.gateway.GatewayTestBase;
import com.google.common.base.Charsets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.SimpleEvent;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test netty flume collector.
 */
public class NettyFlumeCollectorTest extends GatewayTestBase {
  private static final String HOSTNAME = "localhost";
  private static final Gson GSON = StreamEventTypeAdapter.register(new GsonBuilder()).create();

  @Test
  public void testFlumeEnqueue() throws Exception {
    CConfiguration cConfig = GatewayTestBase.getInjector().getInstance(CConfiguration.class);
    cConfig.setInt(Constants.Gateway.STREAM_FLUME_PORT, 0);
    NettyFlumeCollector flumeCollector = GatewayTestBase.getInjector().getInstance(NettyFlumeCollector.class);
    flumeCollector.startAndWait();
    int port = flumeCollector.getPort();

    String streamName = "flume-stream";
    // Create new stream.
    HttpResponse response = GatewayFastTestsSuite.doPut("/v2/streams/" + streamName);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    EntityUtils.consume(response.getEntity());

    // Send flume events
    int totalEvents = 12;
    // Send 6 events one by one
    for (int i = 0; i < totalEvents / 2; i++) {
      sendFlumeEvent(port, createFlumeEvent(i, streamName));
    }

    // Send 6 events in a batch
    sendFlumeEvents(port, streamName, totalEvents / 2, totalEvents);

    flumeCollector.stopAndWait();

    // Get all stream events
    response = GatewayFastTestsSuite.doGet("/v2/streams/" + streamName + "/events?limit=13");
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    long lastEventTime = 0L;
    Reader reader = new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8);
    try {
      List<StreamEvent> events = GSON.fromJson(reader, new TypeToken<List<StreamEvent>>() { }.getType());
      Assert.assertEquals(totalEvents, events.size());
      for (int i = 0; i < totalEvents; i++) {
        StreamEvent event = events.get(i);
        Assert.assertEquals(Integer.toString(i), event.getHeaders().get("messageNumber"));
        Assert.assertEquals("This is a message " + i, Charsets.UTF_8.decode(event.getBody()).toString());

        lastEventTime = event.getTimestamp();
      }
    } finally {
      reader.close();
    }

    // Fetch again, there should be no more events
    response = GatewayFastTestsSuite.doGet("/v2/streams/" + streamName + "/events?start=" + (lastEventTime + 1L));
    Assert.assertEquals(HttpResponseStatus.NO_CONTENT.getCode(), response.getStatusLine().getStatusCode());
  }

  public static SimpleEvent createFlumeEvent(int i, String dest) {
    SimpleEvent event = new SimpleEvent();
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("messageNumber", Integer.toString(i));
    headers.put(Constants.Gateway.HEADER_DESTINATION_STREAM, dest);
    Header authHeader = GatewayTestBase.getAuthHeader();
    headers.put(authHeader.getName(), authHeader.getValue());
    event.setHeaders(headers);
    event.setBody(Bytes.toBytes("This is a message " + i));
    return event;
  }

  public static void sendFlumeEvent(int port, SimpleEvent event)
    throws EventDeliveryException {
    RpcClient client = RpcClientFactory.getDefaultInstance(HOSTNAME, port, 1);
    try {
      client.append(event);
    } catch (EventDeliveryException e) {
      client.close();
      throw e;
    }
    client.close();
  }

  public static void sendFlumeEvents(int port, String dest, int begin, int end) throws EventDeliveryException {
    RpcClient client = RpcClientFactory.getDefaultInstance(HOSTNAME, port, 100);
    try {
      List<Event> events = new ArrayList<Event>();
      for (int i = begin; i < end; ++i) {
        events.add(createFlumeEvent(i, dest));
      }
      client.appendBatch(events);
    } catch (EventDeliveryException e) {
      client.close();
      throw e;
    }
    client.close();
  }
}
