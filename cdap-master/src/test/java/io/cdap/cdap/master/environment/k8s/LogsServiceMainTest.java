/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HttpHeaders;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.logging.appender.LogMessage;
import io.cdap.cdap.logging.context.WorkflowLoggingContext;
import io.cdap.cdap.logging.gateway.handlers.FormattedTextLogEvent;
import io.cdap.cdap.logging.gateway.handlers.LogData;
import io.cdap.cdap.logging.read.LogOffset;
import io.cdap.cdap.logging.serialize.LoggingEventSerializer;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for {@link LogsServiceMain}.
 */
public class LogsServiceMainTest extends MasterServiceMainTestBase {
  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(LogOffset.class, new LogOffsetAdapter()).create();
  private static final Type LIST_LOGDATA_OFFSET_TYPE = new TypeToken<List<LogDataOffset>>() { }.getType();

  @Test
  public void testLogsService() throws Exception {
    Injector injector = getServiceMainInstance(LogsServiceMain.class).getInjector();
    DiscoveryServiceClient client = injector.getInstance(DiscoveryServiceClient.class);

    // send logs to log saver
    TestAppender testAppender = new TestAppender(client);
    for (ILoggingEvent event : getLoggingEvents()) {
      testAppender.append(event);
    }

    // Query the appended logs, we can not query logs for a given run because run record does not exist
    String url = "/v3/namespaces/default/apps/app1/workflows/myworkflow/logs?format=json";
    Tasks.waitFor(true, () -> {
      HttpResponse response = doGet(url, client, Constants.Service.LOG_QUERY);
      if (response.getResponseCode() == 200) {
        List<LogDataOffset> list = GSON.fromJson(response.getResponseBodyAsString(), LIST_LOGDATA_OFFSET_TYPE);
        return list.size() == 6 && list.get(5).log.getMessage().equals("5");
      }
      return false;
    }, 5, TimeUnit.MINUTES, 500, TimeUnit.MILLISECONDS);

    // make sure log saver status service has started
    String statusUrl = "/v3/system/services/log.saver/status";
    HttpResponse response = doGet(statusUrl, client, Constants.Service.LOGSAVER);
    Assert.assertEquals(200, response.getResponseCode());
  }

  private HttpResponse doGet(String path, DiscoveryServiceClient discoveryServiceClient,
                             String serviceName) throws IOException {
    Discoverable discoverable = new RandomEndpointStrategy(
      () -> discoveryServiceClient.discover(serviceName)).pick(10, TimeUnit.SECONDS);
    Assert.assertNotNull(discoverable);

    InetSocketAddress addr = discoverable.getSocketAddress();
    URL url = new URL("http", addr.getHostName(), addr.getPort(), path);
    return HttpRequests.execute(HttpRequest.get(url).build());
  }

  private ImmutableList<ILoggingEvent> getLoggingEvents() {
    WorkflowLoggingContext loggingContext =
      new WorkflowLoggingContext("default", "app1", "myworkflow", "82b25495-35a2-11e9-b987-acde48001122");
    long now = System.currentTimeMillis();
    return ImmutableList.of(
      createLoggingEvent(Level.INFO, "0", now - 1000, loggingContext),
      createLoggingEvent(Level.INFO, "1", now - 900, loggingContext),
      createLoggingEvent(Level.INFO, "2", now - 700, loggingContext),
      createLoggingEvent(Level.DEBUG, "3", now - 600, loggingContext),
      createLoggingEvent(Level.INFO, "4", now - 500, loggingContext),
      createLoggingEvent(Level.INFO, "5", now - 100, loggingContext));
  }

  private ILoggingEvent createLoggingEvent(Level level, String message, long timestamp, LoggingContext loggingContext) {
    LoggingEvent event = new LoggingEvent();
    event.setLevel(level);
    event.setLoggerName("test.logger");
    event.setMessage(message);
    event.setTimeStamp(timestamp);
    return new LogMessage(event, loggingContext);
  }

  private static final class LogOffsetAdapter extends TypeAdapter<LogOffset> {
    @Override
    public void write(JsonWriter out, LogOffset value) throws IOException {
      out.value(FormattedTextLogEvent.formatLogOffset(value));
    }

    @Override
    public LogOffset read(JsonReader in) throws IOException {
      return FormattedTextLogEvent.parseLogOffset(in.nextString());
    }
  }

  private class LogDataOffset extends OffsetLine {
    private final LogData log;

    LogDataOffset(LogData log, LogOffset offset) {
      super(offset);
      this.log = log;
    }

    LogData getLog() {
      return log;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("log", log)
        .add("offset", getOffset())
        .toString();
    }
  }

  private class OffsetLine {
    private final LogOffset offset;

    OffsetLine(LogOffset offset) {
      this.offset = offset;
    }

    LogOffset getOffset() {
      return offset;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("offset", offset)
        .toString();
    }
  }

  private class TestAppender extends AppenderBase<ILoggingEvent> {
    private final LoggingEventSerializer loggingEventSerializer;
    private final RemoteClient remoteClient;
    private final DatumWriter<List<ByteBuffer>> datumWriter;

    private TestAppender(DiscoveryServiceClient client) {
      this.remoteClient = new RemoteClient(client, Constants.Service.LOG_BUFFER_SERVICE,
                                           new DefaultHttpRequestConfig(), "/v1/logs");
      this.loggingEventSerializer = new LoggingEventSerializer();
      this.datumWriter = new GenericDatumWriter<>(Schema.createArray(Schema.create(Schema.Type.BYTES)));
    }

    @Override
    protected void append(ILoggingEvent event) {
      try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
        Encoder encoder = EncoderFactory.get().directBinaryEncoder(os, null);
        datumWriter.write(ImmutableList.of(ByteBuffer.wrap(loggingEventSerializer.toBytes(event))), encoder);
        HttpRequest request = remoteClient.requestBuilder(HttpMethod.POST, "/partitions/0/publish")
          .addHeader(HttpHeaders.CONTENT_TYPE, "avro/binary")
          .withBody(ByteBuffer.wrap(os.toByteArray())).build();
        HttpResponse response = remoteClient.execute(request);
        // if something went wrong, throw exception to retry
        if (response.getResponseCode() != HttpURLConnection.HTTP_OK) {
          throw new IOException("Could not append logs for partition 0");
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
