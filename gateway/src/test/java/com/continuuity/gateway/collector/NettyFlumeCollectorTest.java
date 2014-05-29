package com.continuuity.gateway.collector;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.gateway.GatewayFastTestsSuite;
import com.continuuity.gateway.GatewayTestBase;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.SimpleEvent;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test netty flume collector.
 */
public class NettyFlumeCollectorTest extends GatewayTestBase {
  private static final String hostname = "localhost";

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
    sendFlumeEvent(port, createFlumeEvent(0, streamName));
    sendFlumeEvent(port, createFlumeEvent(1, streamName));
    sendFlumeEvent(port, createFlumeEvent(2, streamName));
    sendFlumeEvent(port, createFlumeEvent(3, streamName));

    sendFlumeEvents(port, streamName, 4, 10);

    sendFlumeEvent(port, createFlumeEvent(11, streamName));
    sendFlumeEvent(port, createFlumeEvent(12, streamName));

    flumeCollector.stopAndWait();

    // Get new consumer id
    response = GatewayFastTestsSuite.doPost("/v2/streams/" + streamName + "/consumer-id", null);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    Assert.assertEquals(1, response.getHeaders(Constants.Stream.Headers.CONSUMER_ID).length);
    String groupId = response.getFirstHeader(Constants.Stream.Headers.CONSUMER_ID).getValue();

    // Dequeue all entries
    for (int i = 0; i <= 12; ++i) {
      response = GatewayFastTestsSuite.doPost("/v2/streams/" + streamName + "/dequeue", null,
                                             new Header[]{
                                               new BasicHeader(Constants.Stream.Headers.CONSUMER_ID, groupId)
                                             });
      Assert.assertEquals("Item " + i, HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
      String actual = EntityUtils.toString(response.getEntity());
      Assert.assertEquals(Integer.toString(i), response.getFirstHeader("flume-stream.messageNumber").getValue());
      Assert.assertEquals("This is a message " + i, actual);
    }

    // No more content
    response = GatewayFastTestsSuite.doPost("/v2/streams/" + streamName + "/dequeue", null,
                                           new Header[]{
                                             new BasicHeader(Constants.Stream.Headers.CONSUMER_ID, groupId)
                                           });
    Assert.assertEquals("No item", HttpResponseStatus.NO_CONTENT.getCode(), response.getStatusLine().getStatusCode());
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
    RpcClient client = RpcClientFactory.getDefaultInstance(hostname, port, 1);
    try {
      client.append(event);
    } catch (EventDeliveryException e) {
      client.close();
      throw e;
    }
    client.close();
  }

  public static void sendFlumeEvents(int port, String dest, int begin, int end) throws EventDeliveryException {
    RpcClient client = RpcClientFactory.getDefaultInstance(hostname, port, 100);
    try {
      List<Event> events = new ArrayList<Event>();
      for (int i = begin; i <= end; ++i) {
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
