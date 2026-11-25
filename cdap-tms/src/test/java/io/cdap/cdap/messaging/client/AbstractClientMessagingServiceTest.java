/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.messaging.client;

import com.google.common.net.HttpHeaders;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.messaging.DefaultMessageFetchRequest;
import io.cdap.cdap.messaging.Schemas;
import io.cdap.cdap.messaging.spi.MessageFetchRequest;
import io.cdap.cdap.messaging.spi.RawMessage;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.common.http.HttpMethod;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link AbstractClientMessagingService} Guava call paths.
 */
public class AbstractClientMessagingServiceTest {

  @Test
  public void testFetchCloseQuietlySwallowsStreamErrorAndDisconnects() throws Exception {
    TopicId topicId = NamespaceId.DEFAULT.topic("t1");
    byte[] encodedResponse = encodeSingleMessage(Bytes.toBytes("id-1"), Bytes.toBytes("hello"));
    final AtomicBoolean streamCloseAttempted = new AtomicBoolean(false);
    final AtomicBoolean disconnected = new AtomicBoolean(false);

    InputStream failingCloseStream = new FilterInputStream(new ByteArrayInputStream(encodedResponse)) {
      @Override
      public void close() throws IOException {
        streamCloseAttempted.set(true);
        throw new IOException("Simulated input stream close failure");
      }
    };

    StubHttpUrlConnection connection = new StubHttpUrlConnection(
        new URL("http://localhost/v3/namespaces/default/topics/t1/poll"),
        HttpURLConnection.HTTP_OK,
        failingCloseStream,
        disconnected);

    RemoteClient remoteClient = Mockito.mock(RemoteClient.class);
    Mockito.when(remoteClient.openConnection(HttpMethod.POST, "default/topics/t1/poll"))
        .thenReturn(connection);

    AbstractClientMessagingService service = new TestClientMessagingService(remoteClient);
    MessageFetchRequest fetchRequest = new DefaultMessageFetchRequest.Builder()
        .setTopicId(topicId)
        .build();

    CloseableIterator<RawMessage> iterator = service.fetch(fetchRequest);
    Assert.assertTrue(iterator.hasNext());
    RawMessage message = iterator.next();
    Assert.assertEquals("id-1", Bytes.toString(message.getId()));
    Assert.assertEquals("hello", Bytes.toString(message.getPayload()));
    Assert.assertFalse(iterator.hasNext());

    // Closing must swallow the IOException from inputStream.close() and still disconnect urlConn
    iterator.close();
    Assert.assertTrue(streamCloseAttempted.get());
    Assert.assertTrue(disconnected.get());
  }

  @Test
  public void testFetchComputeNextPropagatesIoExceptionAsRuntimeException() throws Exception {
    TopicId topicId = NamespaceId.DEFAULT.topic("t2");
    // Encode an Avro array header claiming 1 element, followed by no element bytes so read() throws EOFException
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(bos, null);
    encoder.writeArrayStart();
    encoder.setItemCount(1);
    encoder.flush();

    final AtomicBoolean disconnected = new AtomicBoolean(false);
    StubHttpUrlConnection connection = new StubHttpUrlConnection(
        new URL("http://localhost/v3/namespaces/default/topics/t2/poll"),
        HttpURLConnection.HTTP_OK,
        new ByteArrayInputStream(bos.toByteArray()),
        disconnected);

    RemoteClient remoteClient = Mockito.mock(RemoteClient.class);
    Mockito.when(remoteClient.openConnection(HttpMethod.POST, "default/topics/t2/poll"))
        .thenReturn(connection);

    AbstractClientMessagingService service = new TestClientMessagingService(remoteClient);
    MessageFetchRequest fetchRequest = new DefaultMessageFetchRequest.Builder()
        .setTopicId(topicId)
        .build();

    try (CloseableIterator<RawMessage> iterator = service.fetch(fetchRequest)) {
      try {
        iterator.hasNext();
        Assert.fail("Expected RuntimeException wrapping IOException");
      } catch (RuntimeException e) {
        Assert.assertTrue("Expected IOException cause, got: " + e.getCause(),
            e.getCause() instanceof IOException);
      }
    }
    Assert.assertTrue(disconnected.get());
  }

  private static byte[] encodeSingleMessage(byte[] id, byte[] payload) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(bos, null);
    encoder.writeArrayStart();
    encoder.setItemCount(1);
    encoder.startItem();

    GenericRecord element = new GenericData.Record(Schemas.V1.ConsumeResponse.SCHEMA.getElementType());
    element.put("id", ByteBuffer.wrap(id));
    element.put("payload", ByteBuffer.wrap(payload));
    DatumWriter<GenericRecord> writer =
        new GenericDatumWriter<>(Schemas.V1.ConsumeResponse.SCHEMA.getElementType());
    writer.write(element, encoder);

    encoder.writeArrayEnd();
    encoder.flush();
    return bos.toByteArray();
  }

  private static final class TestClientMessagingService extends AbstractClientMessagingService {
    TestClientMessagingService(RemoteClient remoteClient) {
      super(remoteClient, false);
    }
  }

  private static final class StubHttpUrlConnection extends HttpURLConnection {
    private final int statusCode;
    private final InputStream responseStream;
    private final AtomicBoolean disconnected;
    private final ByteArrayOutputStream requestOutput = new ByteArrayOutputStream();

    StubHttpUrlConnection(URL u, int statusCode, InputStream responseStream, AtomicBoolean disconnected) {
      super(u);
      this.statusCode = statusCode;
      this.responseStream = responseStream;
      this.disconnected = disconnected;
    }

    @Override
    public void disconnect() {
      disconnected.set(true);
    }

    @Override
    public boolean usingProxy() {
      return false;
    }

    @Override
    public void connect() {
      // no-op
    }

    @Override
    public OutputStream getOutputStream() {
      return requestOutput;
    }

    @Override
    public int getResponseCode() {
      return statusCode;
    }

    @Override
    public Map<String, List<String>> getHeaderFields() {
      return Collections.singletonMap(
          HttpHeaders.CONTENT_TYPE, Collections.singletonList("avro/binary"));
    }

    @Override
    public String getHeaderField(String name) {
      if (HttpHeaders.CONTENT_TYPE.equalsIgnoreCase(name)) {
        return "avro/binary";
      }
      return null;
    }

    @Override
    public InputStream getInputStream() {
      return responseStream;
    }
  }
}
