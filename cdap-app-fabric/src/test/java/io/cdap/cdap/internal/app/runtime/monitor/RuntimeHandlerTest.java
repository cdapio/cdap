/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.http.BodyConsumer;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.ws.rs.core.MediaType;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link RuntimeHandler}.
 */
public class RuntimeHandlerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testWriteMessagesStreamingChunksAndFinishedCleanup() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    String logTopic = cConf.get(Constants.Logging.TMS_TOPIC_PREFIX) + "0";
    cConf.set(Constants.RuntimeMonitor.TOPICS_CONFIGS, Constants.Logging.TMS_TOPIC_PREFIX + ":1");

    List<String> receivedMessages = new ArrayList<>();
    RemoteExecutionLogProcessor logProcessor = payloads -> {
      while (payloads.hasNext()) {
        receivedMessages.add(new String(payloads.next(), StandardCharsets.UTF_8));
      }
    };
    RuntimeRequestValidator validator =
        (programRunId, request) -> new ProgramRunInfo(ProgramRunStatus.RUNNING);

    RuntimeHandler handler = new RuntimeHandler(
        cConf,
        Mockito.mock(MessagingService.class),
        logProcessor,
        validator,
        new LocalLocationFactory(TEMP_FOLDER.newFolder()));

    DefaultHttpRequest request =
        new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/topics/" + logTopic);
    request.headers().set(HttpHeaderNames.CONTENT_TYPE, "avro/binary");

    HttpResponder responder = Mockito.mock(HttpResponder.class);
    BodyConsumer consumer = handler.writeMessages(
        request, responder, "default", "app1", "-SNAPSHOT", "workflows", "wf1", "run1", logTopic);

    byte[] avroPayload = encodeAvroByteArray(Arrays.asList("first-log", "second-log"));
    int split = avroPayload.length / 2;
    ByteBuf chunk1 = Unpooled.wrappedBuffer(avroPayload, 0, split);
    ByteBuf chunk2 = Unpooled.wrappedBuffer(avroPayload, split, avroPayload.length - split);

    // Multiple chunk() calls exercise DelegatingInputStream#setDelegate closing previous stream quietly.
    consumer.chunk(chunk1, responder);
    consumer.chunk(chunk2, responder);
    consumer.finished(responder);

    Assert.assertEquals(Arrays.asList("first-log", "second-log"), receivedMessages);
    Mockito.verify(responder).sendJson(Mockito.eq(HttpResponseStatus.OK), Mockito.anyString());
  }

  @Test
  public void testWriteSparkEventLogsHandleErrorClosesStreamQuietlyAndDeletesLocation() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(Constants.AppFabric.SPARK_EVENT_LOGS_ENABLED, true);
    cConf.set(Constants.AppFabric.SPARK_EVENT_LOGS_DIR, "spark-events");

    File rootDir = TEMP_FOLDER.newFolder();
    RuntimeRequestValidator validator =
        (programRunId, request) -> new ProgramRunInfo(ProgramRunStatus.RUNNING);

    // 1. Real LocalLocationFactory: verify file is closed and deleted on handleError.
    RuntimeHandler handler = new RuntimeHandler(
        cConf,
        Mockito.mock(MessagingService.class),
        payloads -> { },
        validator,
        new LocalLocationFactory(rootDir));

    DefaultHttpRequest request =
        new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/spark-event-logs/1");
    request.headers().set(HttpHeaderNames.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM);

    HttpResponder responder = Mockito.mock(HttpResponder.class);
    BodyConsumer consumer = handler.writeSparkEventLogs(
        request, responder, "default", "app1", "-SNAPSHOT", "spark", "sp1", "run1", "evt1");
    consumer.chunk(Unpooled.copiedBuffer("event-data", StandardCharsets.UTF_8), responder);
    consumer.handleError(new IOException("broken pipe"));

    File expectedEventFile = new File(new File(rootDir, "spark-events"), "default-app1-sp1-run1-evt1");
    Assert.assertFalse(expectedEventFile.exists());

    // 2. Mock Location whose OutputStream#close throws IOException: handleError must close quietly and delete.
    AtomicBoolean osClosed = new AtomicBoolean(false);
    OutputStream throwingCloseStream = new OutputStream() {
      @Override
      public void write(int b) {
        // no-op
      }

      @Override
      public void close() throws IOException {
        osClosed.set(true);
        throw new IOException("close failure");
      }
    };

    Location baseLocation = Mockito.mock(Location.class);
    Location eventFileLocation = Mockito.mock(Location.class);
    Mockito.when(baseLocation.append(Mockito.anyString())).thenReturn(eventFileLocation);
    Mockito.when(eventFileLocation.exists()).thenReturn(false);
    Mockito.when(eventFileLocation.getOutputStream()).thenReturn(throwingCloseStream);

    LocationFactory mockLocationFactory = Mockito.mock(LocationFactory.class);
    Mockito.when(mockLocationFactory.create(Mockito.anyString())).thenReturn(baseLocation);

    RuntimeHandler mockLocHandler = new RuntimeHandler(
        cConf,
        Mockito.mock(MessagingService.class),
        payloads -> { },
        validator,
        mockLocationFactory);

    BodyConsumer failingCloseConsumer = mockLocHandler.writeSparkEventLogs(
        request, responder, "default", "app1", "-SNAPSHOT", "spark", "sp1", "run2", "evt2");
    failingCloseConsumer.handleError(new RuntimeException("stream failed"));
    Assert.assertTrue(osClosed.get());
    Mockito.verify(eventFileLocation).delete();
  }

  private byte[] encodeAvroByteArray(List<String> items) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
    encoder.writeArrayStart();
    encoder.setItemCount(items.size());
    for (String item : items) {
      encoder.startItem();
      encoder.writeBytes(ByteBuffer.wrap(item.getBytes(StandardCharsets.UTF_8)));
    }
    encoder.writeArrayEnd();
    encoder.flush();
    return out.toByteArray();
  }
}
