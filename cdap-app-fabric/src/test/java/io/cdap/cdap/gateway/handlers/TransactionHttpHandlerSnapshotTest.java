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

package io.cdap.cdap.gateway.handlers;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.http.BodyProducer;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionSystemClient;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit tests for {@link TransactionHttpHandler#getTxManagerSnapshot(HttpRequest, HttpResponder)}.
 */
public class TransactionHttpHandlerSnapshotTest {

  @Test
  public void testSnapshotStreamClosedQuietlyOnFinishedAndHandleError() throws Exception {
    byte[] snapshotBytes = "tx-snapshot-data".getBytes(StandardCharsets.UTF_8);

    // 1. BodyProducer.finished() closes stream quietly even if close() throws IOException.
    AtomicInteger finishedCloseCount = new AtomicInteger();
    InputStream finishedStream = createTrackingStream(snapshotBytes, finishedCloseCount, true);
    TransactionSystemClient txClient = Mockito.mock(TransactionSystemClient.class);
    Mockito.when(txClient.getSnapshotInputStream()).thenReturn(finishedStream);

    TransactionHttpHandler handler =
        new TransactionHttpHandler(new Configuration(), CConfiguration.create(), txClient);
    HttpRequest request = new DefaultHttpRequest(
        HttpVersion.HTTP_1_1, HttpMethod.GET, "/v3/transactions/state");
    HttpResponder responder = Mockito.mock(HttpResponder.class);

    handler.getTxManagerSnapshot(request, responder);
    ArgumentCaptor<BodyProducer> producerCaptor = ArgumentCaptor.forClass(BodyProducer.class);
    Mockito.verify(responder).sendContent(
        Mockito.eq(HttpResponseStatus.OK), producerCaptor.capture(),
        Mockito.eq(EmptyHttpHeaders.INSTANCE));

    BodyProducer producer = producerCaptor.getValue();
    ByteBuf chunk = producer.nextChunk();
    try {
      Assert.assertEquals("tx-snapshot-data", chunk.toString(StandardCharsets.UTF_8));
    } finally {
      chunk.release();
    }
    producer.finished();
    Assert.assertEquals(1, finishedCloseCount.get());

    // 2. BodyProducer.handleError(...) closes stream quietly even if close() throws IOException.
    AtomicInteger errorCloseCount = new AtomicInteger();
    InputStream errorStream = createTrackingStream(snapshotBytes, errorCloseCount, true);
    Mockito.when(txClient.getSnapshotInputStream()).thenReturn(errorStream);

    HttpResponder secondResponder = Mockito.mock(HttpResponder.class);
    handler.getTxManagerSnapshot(request, secondResponder);
    ArgumentCaptor<BodyProducer> errorCaptor = ArgumentCaptor.forClass(BodyProducer.class);
    Mockito.verify(secondResponder).sendContent(
        Mockito.eq(HttpResponseStatus.OK), errorCaptor.capture(),
        Mockito.eq(EmptyHttpHeaders.INSTANCE));

    errorCaptor.getValue().handleError(new IOException("channel closed"));
    Assert.assertEquals(1, errorCloseCount.get());
  }

  @Test
  public void testSnapshotStreamClosedQuietlyWhenSendContentThrows() throws Exception {
    byte[] snapshotBytes = "tx-snapshot".getBytes(StandardCharsets.UTF_8);
    AtomicInteger closeCount = new AtomicInteger();
    InputStream stream = createTrackingStream(snapshotBytes, closeCount, true);

    TransactionSystemClient txClient = Mockito.mock(TransactionSystemClient.class);
    Mockito.when(txClient.getSnapshotInputStream()).thenReturn(stream);

    TransactionHttpHandler handler =
        new TransactionHttpHandler(new Configuration(), CConfiguration.create(), txClient);
    HttpRequest request = new DefaultHttpRequest(
        HttpVersion.HTTP_1_1, HttpMethod.GET, "/v3/transactions/state");
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    IllegalStateException expected = new IllegalStateException("responder disconnected");
    Mockito.doThrow(expected).when(responder)
        .sendContent(Mockito.any(), Mockito.any(BodyProducer.class), Mockito.any());

    try {
      handler.getTxManagerSnapshot(request, responder);
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expected, actual);
    }
    Assert.assertEquals(1, closeCount.get());
  }

  private static InputStream createTrackingStream(
      byte[] data, AtomicInteger closeCount, boolean throwOnClose) {
    return new ByteArrayInputStream(data) {
      @Override
      public void close() throws IOException {
        closeCount.incrementAndGet();
        if (throwOnClose) {
          throw new IOException("simulated close error");
        }
        super.close();
      }
    };
  }
}
