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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.stream.StreamEventTypeAdapter;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.format.TextRecordFormat;
import co.cask.cdap.gateway.GatewayFastTestsSuite;
import co.cask.cdap.gateway.GatewayTestBase;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.MetricQueryResult;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.StreamDetail;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.commons.lang3.ArrayUtils;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Test stream handler. This is not part of GatewayFastTestsSuite because it needs to start the gateway multiple times.
 */
public class StreamHandlerTest extends GatewayTestBase {
  private static final String API_KEY = GatewayTestBase.getAuthHeader().getValue();

  private static final Gson GSON = StreamEventTypeAdapter.register(
    new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter())).create();


  protected URL createURL(String path) throws URISyntaxException, MalformedURLException {
    return createURL(Id.Namespace.DEFAULT.getId(), path);
  }

  protected URL createStreamInfoURL(String streamName) throws URISyntaxException, MalformedURLException {
    return createURL(String.format("streams/%s", streamName));
  }

  protected URL createPropertiesURL(String streamName) throws URISyntaxException, MalformedURLException {
    return createURL(String.format("streams/%s/properties", streamName));
  }

  private URL createURL(String namespace, String path) throws URISyntaxException, MalformedURLException {
    return getEndPoint(String.format("/v3/namespaces/%s/%s", namespace, path)).toURL();
  }

  protected HttpURLConnection openURL(URL url, HttpMethod method) throws IOException {
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    urlConn.setRequestMethod(method.getName());
    urlConn.setRequestProperty(Constants.Gateway.API_KEY, API_KEY);
    return urlConn;
  }

  @After
  public void reset() throws Exception {
    org.apache.http.HttpResponse httpResponse = GatewayFastTestsSuite.doDelete("/v3/unrecoverable/namespaces/default");
    Assert.assertEquals(200, httpResponse.getStatusLine().getStatusCode());
  }

  @Test
  public void testStreamCreateInvalidName() throws Exception {
    // Now, create the new stream with an invalid character: '@'
    HttpURLConnection urlConn = openURL(createURL("streams/inv@lidStreamName"), HttpMethod.PUT);
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.getCode(), urlConn.getResponseCode());
    urlConn.disconnect();
  }

  @Test
  public void testUpdateDescription() throws Exception {
    // Create a stream with some ttl and description
    String desc = "large stream";
    HttpURLConnection urlConn = openURL(createURL("streams/stream1"), HttpMethod.PUT);
    urlConn.setDoOutput(true);
    Schema schema = Schema.recordOf("event", Schema.Field.of("purchase", Schema.of(Schema.Type.STRING)));
    FormatSpecification formatSpecification = new FormatSpecification(
      TextRecordFormat.class.getCanonicalName(), schema, ImmutableMap.of(TextRecordFormat.CHARSET, "utf8"));
    StreamProperties properties = new StreamProperties(1L, formatSpecification, 128, desc);
    urlConn.getOutputStream().write(GSON.toJson(properties).getBytes(Charsets.UTF_8));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
    urlConn.disconnect();

    // Check whether ttl and description got persisted
    urlConn = openURL(createStreamInfoURL("stream1"), HttpMethod.GET);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
    StreamProperties actual = GSON.fromJson(new String(ByteStreams.toByteArray(urlConn.getInputStream()),
                                                       Charsets.UTF_8), StreamProperties.class);
    urlConn.disconnect();
    Assert.assertEquals(properties, actual);

    // Update desc and ttl and check whether the changes were persisted
    StreamProperties newProps = new StreamProperties(2L, null, null, "small stream");
    urlConn = openURL(createPropertiesURL("stream1"), HttpMethod.PUT);
    urlConn.setDoOutput(true);
    urlConn.getOutputStream().write(GSON.toJson(newProps).getBytes(Charsets.UTF_8));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
    urlConn.disconnect();

    urlConn = openURL(createStreamInfoURL("stream1"), HttpMethod.GET);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
    actual = GSON.fromJson(new String(ByteStreams.toByteArray(urlConn.getInputStream()), Charsets.UTF_8),
                           StreamProperties.class);
    urlConn.disconnect();
    StreamProperties expected = new StreamProperties(newProps.getTTL(), properties.getFormat(),
                                                     properties.getNotificationThresholdMB(),
                                                     newProps.getDescription());
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testStreamCreate() throws Exception {
    // Try to get info on a non-existent stream
    HttpURLConnection urlConn = openURL(createStreamInfoURL("test_stream1"), HttpMethod.GET);

    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), urlConn.getResponseCode());
    urlConn.disconnect();

    // try to POST info to the non-existent stream
    urlConn = openURL(createURL("streams/non_existent_stream"), HttpMethod.POST);
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), urlConn.getResponseCode());
    urlConn.disconnect();

    // Now, create the new stream.
    urlConn = openURL(createURL("streams/test_stream1"), HttpMethod.PUT);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
    urlConn.disconnect();

    // getInfo should now return 200
    urlConn = openURL(createStreamInfoURL("test_stream1"), HttpMethod.GET);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
    urlConn.disconnect();
  }

  @Test
  public void testSimpleStreamEnqueue() throws Exception {
    // Create new stream.
    HttpURLConnection urlConn = openURL(createURL("streams/test_stream_enqueue"), HttpMethod.PUT);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
    urlConn.disconnect();

    // Enqueue 10 entries
    for (int i = 0; i < 10; ++i) {
      urlConn = openURL(createURL("streams/test_stream_enqueue"), HttpMethod.POST);
      urlConn.setDoOutput(true);
      urlConn.addRequestProperty("test_stream_enqueue.header1", Integer.toString(i));
      urlConn.getOutputStream().write(Integer.toString(i).getBytes(Charsets.UTF_8));
      Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
      urlConn.disconnect();
    }

    // Fetch 10 entries
    urlConn = openURL(createURL("streams/test_stream_enqueue/events?limit=10"), HttpMethod.GET);
    List<StreamEvent> events = GSON.fromJson(new String(ByteStreams.toByteArray(urlConn.getInputStream()),
                                                        Charsets.UTF_8),
                                             new TypeToken<List<StreamEvent>>() { }.getType());
    for (int i = 0; i < 10; i++) {
      StreamEvent event = events.get(i);
      int actual = Integer.parseInt(Charsets.UTF_8.decode(event.getBody()).toString());
      Assert.assertEquals(i, actual);
      Assert.assertEquals(Integer.toString(i), event.getHeaders().get("header1"));
    }
    urlConn.disconnect();
  }

  @Test
  public void testListStreams() throws Exception {
    List<StreamDetail> specs = listStreams(NamespaceId.DEFAULT);
    Assert.assertTrue(specs.isEmpty());
    StreamId s1 = NamespaceId.DEFAULT.stream("stream1");
    StreamId s2 = NamespaceId.DEFAULT.stream("stream2");
    createStream(s1.toId());
    specs = listStreams(NamespaceId.DEFAULT);
    Assert.assertEquals(1, specs.size());
    StreamDetail specification = specs.get(0);
    Assert.assertEquals(s1.getStream(), specification.getName());
    createStream(s2.toId());
    specs = listStreams(NamespaceId.DEFAULT);
    Assert.assertEquals(2, specs.size());
    try {
      listStreams(new NamespaceId("notfound"));
      Assert.fail("Should have thrown NamespaceNotFoundException");
    } catch (NamespaceNotFoundException ex) {
      // expected
    }
  }

  @Test
  public void testStreamInfo() throws Exception {
    // Now, create the new stream.
    HttpURLConnection urlConn = openURL(createURL("streams/stream_info"), HttpMethod.PUT);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
    urlConn.disconnect();

    // put a new config
    urlConn = openURL(createPropertiesURL("stream_info"), HttpMethod.PUT);
    urlConn.setDoOutput(true);
    Schema schema = Schema.recordOf("event", Schema.Field.of("purchase", Schema.of(Schema.Type.STRING)));
    FormatSpecification formatSpecification;
    formatSpecification = new FormatSpecification(TextRecordFormat.class.getCanonicalName(),
                            schema,
                            ImmutableMap.of(TextRecordFormat.CHARSET, "utf8"));
    StreamProperties streamProperties = new StreamProperties(2L, formatSpecification, 20);
    urlConn.getOutputStream().write(GSON.toJson(streamProperties).getBytes(Charsets.UTF_8));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
    urlConn.disconnect();

    // test the config ttl by calling info
    urlConn = openURL(createStreamInfoURL("stream_info"), HttpMethod.GET);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
    StreamProperties actual = GSON.fromJson(new String(ByteStreams.toByteArray(urlConn.getInputStream()),
                                                        Charsets.UTF_8), StreamProperties.class);
    urlConn.disconnect();
    Assert.assertEquals(streamProperties, actual);
  }

  @Test
  public void testPutStreamConfigDefaults() throws Exception {
    // Now, create the new stream.
    HttpURLConnection urlConn = openURL(createURL("streams/stream_defaults"), HttpMethod.PUT);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
    urlConn.disconnect();

    // put a new config
    urlConn = openURL(createPropertiesURL("stream_defaults"), HttpMethod.PUT);
    urlConn.setDoOutput(true);
    // don't give the schema to make sure a default gets used
    FormatSpecification formatSpecification = new FormatSpecification(Formats.TEXT, null, null);
    StreamProperties streamProperties = new StreamProperties(2L, formatSpecification, 20);
    urlConn.getOutputStream().write(GSON.toJson(streamProperties).getBytes(Charsets.UTF_8));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
    urlConn.disconnect();

    // test the config ttl by calling info
    urlConn = openURL(createStreamInfoURL("stream_defaults"),
                      HttpMethod.GET);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
    StreamProperties actual = GSON.fromJson(new String(ByteStreams.toByteArray(urlConn.getInputStream()),
                                                       Charsets.UTF_8), StreamProperties.class);
    urlConn.disconnect();

    StreamProperties expected = new StreamProperties(2L, StreamConfig.DEFAULT_STREAM_FORMAT, 20);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testPutInvalidStreamConfig() throws Exception {
    // create the new stream.
    HttpURLConnection urlConn = openURL(createURL("streams/stream_badconf"), HttpMethod.PUT);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
    urlConn.disconnect();

    // put a config with invalid json
    urlConn = openURL(createPropertiesURL("stream_badconf"), HttpMethod.PUT);
    urlConn.setDoOutput(true);
    urlConn.getOutputStream().write("ttl:2".getBytes(Charsets.UTF_8));
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.getCode(), urlConn.getResponseCode());
    urlConn.disconnect();

    // put a config with an invalid TTL
    urlConn = openURL(createPropertiesURL("stream_badconf"), HttpMethod.PUT);
    urlConn.setDoOutput(true);
    StreamProperties streamProperties = new StreamProperties(-1L, null, 20);
    urlConn.getOutputStream().write(GSON.toJson(streamProperties).getBytes(Charsets.UTF_8));
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.getCode(), urlConn.getResponseCode());
    urlConn.disconnect();

    // put a config with a format without a format class
    urlConn = openURL(createPropertiesURL("stream_badconf"), HttpMethod.PUT);
    urlConn.setDoOutput(true);
    FormatSpecification formatSpec = new FormatSpecification(null, null, null);
    streamProperties = new StreamProperties(2L, formatSpec, 20);
    urlConn.getOutputStream().write(GSON.toJson(streamProperties).getBytes(Charsets.UTF_8));
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.getCode(), urlConn.getResponseCode());
    urlConn.disconnect();

    // put a config with a format with a bad format class
    urlConn = openURL(createPropertiesURL("stream_badconf"), HttpMethod.PUT);
    urlConn.setDoOutput(true);
    formatSpec = new FormatSpecification("gibberish", null, null);
    streamProperties = new StreamProperties(2L, formatSpec, 20);
    urlConn.getOutputStream().write(GSON.toJson(streamProperties).getBytes(Charsets.UTF_8));
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.getCode(), urlConn.getResponseCode());
    urlConn.disconnect();

    // put a config with an incompatible format and schema
    urlConn = openURL(createPropertiesURL("stream_badconf"), HttpMethod.PUT);
    urlConn.setDoOutput(true);
    Schema schema = Schema.recordOf("event", Schema.Field.of("col", Schema.of(Schema.Type.DOUBLE)));
    formatSpec = new FormatSpecification(TextRecordFormat.class.getCanonicalName(), schema, null);
    streamProperties = new StreamProperties(2L, formatSpec, 20);
    urlConn.getOutputStream().write(GSON.toJson(streamProperties).getBytes(Charsets.UTF_8));
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.getCode(), urlConn.getResponseCode());
    urlConn.disconnect();

    // put a config with a bad threshold
    urlConn = openURL(createPropertiesURL("stream_badconf"), HttpMethod.PUT);
    urlConn.setDoOutput(true);
    streamProperties = new StreamProperties(2L, null, -20);
    urlConn.getOutputStream().write(GSON.toJson(streamProperties).getBytes(Charsets.UTF_8));
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.getCode(), urlConn.getResponseCode());
    urlConn.disconnect();
  }

  @Test
  public void testStreamCreateInNonexistentNamespace() throws Exception {
    Id.Namespace originallyNonExistentNamespace = Id.Namespace.from("originallyNonExistentNamespace");
    Id.Stream streamId = Id.Stream.from(originallyNonExistentNamespace, "streamName");
    HttpResponse response = createStream(streamId, 404);
    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), response.getResponseCode());

    // once the namespace exists, the same stream create works.
    namespaceAdmin.create(new NamespaceMeta.Builder().setName(originallyNonExistentNamespace).build());
    createStream(streamId);
  }

  @Test
  public void testNamespacedStreamEvents() throws Exception {
    // Create two streams with the same name, in different namespaces.
    String streamName = "testNamespacedEvents";
    Id.Stream streamId1 = Id.Stream.from(TEST_NAMESPACE1, streamName);
    Id.Stream streamId2 = Id.Stream.from(TEST_NAMESPACE2, streamName);

    createStream(streamId1);
    createStream(streamId2);

    List<String> eventsSentToStream1 = Lists.newArrayList();
    // Enqueue 10 entries to the stream in the first namespace
    for (int i = 0; i < 10; ++i) {
      String body = streamId1.getNamespaceId() + i;
      sendEvent(streamId1, body);
      eventsSentToStream1.add(body);
    }

    List<String> eventsSentToStream2 = Lists.newArrayList();
    // Enqueue only 5 entries to the stream in the second namespace, decrementing the value each time
    for (int i = 0; i > -5; --i) {
      String body = streamId1.getNamespaceId() + i;
      sendEvent(streamId2, body);
      eventsSentToStream2.add(body);
    }

    // Test that even though the stream names are the same, the events ingested into the individual streams
    // are exactly what are fetched from the individual streams.
    List<String> eventsFetchedFromStream1 = fetchEvents(streamId1);
    Assert.assertEquals(eventsSentToStream1, eventsFetchedFromStream1);

    List<String> eventsFetchedFromStream2 = fetchEvents(streamId2);
    Assert.assertEquals(eventsSentToStream2, eventsFetchedFromStream2);
  }

  @Test
  public void testNamespacedMetrics() throws Exception {
    // Create two streams with the same name, in different namespaces.
    String streamName = "testNamespacedStreamMetrics";
    Id.Stream streamId1 = Id.Stream.from(TEST_NAMESPACE1, streamName);
    Id.Stream streamId2 = Id.Stream.from(TEST_NAMESPACE2, streamName);

    createStream(streamId1);
    createStream(streamId2);

    // Enqueue 10 entries to the stream in the first namespace
    for (int i = 0; i < 10; ++i) {
      sendEvent(streamId1, Integer.toString(i));
    }

    // Enqueue only 2 entries to the stream in the second namespace
    for (int i = 0; i < 2; ++i) {
      sendEvent(streamId2, Integer.toString(i));
    }

    // Check metrics to verify that the metric for events processed is specific to each stream
    checkEventsProcessed(streamId1, 10L, 10);
    checkEventsProcessed(streamId2, 2L, 10);
  }


  private HttpResponse createStream(Id.Stream streamId, int... allowedErrorCodes) throws Exception {
    URL url = createURL(streamId.getNamespaceId(), "streams/" + streamId.getId());
    HttpRequest request = HttpRequest.put(url).build();
    HttpResponse response = HttpRequests.execute(request);
    int responseCode = response.getResponseCode();
    if (!ArrayUtils.contains(allowedErrorCodes, responseCode)) {
      Assert.assertEquals(200, responseCode);
    }
    return response;
  }

  private List<StreamDetail> listStreams(NamespaceId namespaceId) throws Exception {
    URL url = createURL(namespaceId.getNamespace(), "streams");
    HttpRequest request = HttpRequest.get(url).build();
    HttpResponse response = HttpRequests.execute(request);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NamespaceNotFoundException(namespaceId.toId());
    }
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), new TypeToken<List<StreamDetail>>() { }.getType());
  }

  private void sendEvent(Id.Stream streamId, String body) throws Exception {
    URL url = createURL(streamId.getNamespaceId(), "streams/" + streamId.getId());
    HttpRequest request = HttpRequest.post(url).withBody(body).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
  }

  private List<String> fetchEvents(Id.Stream streamId) throws Exception {
    URL url = createURL(streamId.getNamespaceId(), "streams/" + streamId.getId() + "/events");
    HttpRequest request = HttpRequest.get(url).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());

    List<String> events = Lists.newArrayList();
    JsonArray jsonArray = new JsonParser().parse(response.getResponseBodyAsString()).getAsJsonArray();
    for (JsonElement jsonElement : jsonArray) {
      events.add(jsonElement.getAsJsonObject().get("body").getAsString());
    }
    return events;
  }

  private void checkEventsProcessed(final Id.Stream streamId, long expectedCount, int retries) throws Exception {
    Tasks.waitFor(expectedCount, new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        return getNumProcessed(streamId);
      }
    }, retries, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }


  private long getNumProcessed(Id.Stream streamId) throws Exception {
    String path =
      String.format("/v3/metrics/query?metric=system.collect.events&tag=namespace:%s&tag=stream:%s&aggregate=true",
                    streamId.getNamespaceId(), streamId.getId());
    HttpRequest request = HttpRequest.post(getEndPoint(path).toURL()).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
    return getNumEventsFromResponse(response.getResponseBodyAsString());
  }

  private long getNumEventsFromResponse(String response) {
    MetricQueryResult metricQueryResult = new Gson().fromJson(response, MetricQueryResult.class);
    MetricQueryResult.TimeSeries[] series = metricQueryResult.getSeries();

    if (series.length == 0) {
      return 0;
    }
    return series[0].getData()[0].getValue();
  }
}
