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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.stream.StreamEventTypeAdapter;
import co.cask.cdap.data.format.TextRecordFormat;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.gateway.GatewayFastTestsSuite;
import co.cask.cdap.gateway.GatewayTestBase;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.StreamProperties;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.http.HttpResponse;
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

/**
 * Test stream handler. This is not part of GatewayFastTestsSuite because it needs to start the gateway multiple times.
 */
public abstract class StreamHandlerTest extends GatewayTestBase {
  private static final String API_KEY = GatewayTestBase.getAuthHeader().getValue();

  private static final Gson GSON = StreamEventTypeAdapter.register(
    new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter())).create();


  protected abstract URL createURL(String path) throws URISyntaxException, MalformedURLException;

  protected abstract URL createStreamInfoURL(String streamName) throws URISyntaxException, MalformedURLException;

  protected abstract URL createPropertiesURL(String streamName) throws URISyntaxException, MalformedURLException;


  protected HttpURLConnection openURL(URL url, HttpMethod method) throws IOException {
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    urlConn.setRequestMethod(method.getName());
    urlConn.setRequestProperty(Constants.Gateway.API_KEY, API_KEY);

    return urlConn;
  }

  @After
  public void reset() throws Exception {
    HttpResponse httpResponse = GatewayFastTestsSuite.doPost("/v2/unrecoverable/reset", null);
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
  public void testStreamCreate() throws Exception {
    // Try to get info on a non-existent stream
    HttpURLConnection urlConn = openURL(createStreamInfoURL("test_stream1"),
                                        HttpMethod.GET);

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
    HttpURLConnection urlConn = openURL(createURL("streams/test_stream_enqueue"),
                                        HttpMethod.PUT);
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
    urlConn = openURL(createURL("streams/test_stream_enqueue/events?limit=10"),
                      HttpMethod.GET);
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
    urlConn = openURL(createStreamInfoURL("stream_info"),
                      HttpMethod.GET);
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
}
