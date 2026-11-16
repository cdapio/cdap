/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.logging.gateway.handlers;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.LoggingEvent;
import io.cdap.cdap.logging.MockResponder;
import io.cdap.cdap.logging.read.LogEvent;
import io.cdap.cdap.logging.read.LogOffset;
import io.cdap.http.AbstractHttpResponder;
import io.cdap.http.BodyProducer;
import io.cdap.http.ChunkResponder;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link AbstractChunkedCallback}.
 */
public class AbstractChunkedCallbackTest {

  @Test
  public void testNormalChunkedWriteAndIdempotentClose() {
    MockResponder responder = new MockResponder();
    TestChunkedCallback callback = new TestChunkedCallback(responder);

    callback.init();
    callback.init();
    callback.handle(createLogEvent("line-1"));
    callback.handle(createLogEvent("line-2"));
    Assert.assertEquals(2, callback.getCount());

    callback.close();
    callback.close();
    Assert.assertEquals(HttpResponseStatus.OK, responder.getStatus());
    Assert.assertEquals("line-1\nline-2\n", responder.getResponseContentAsString());
  }

  @Test
  public void testCloseQuietlySwallowsSendChunkAndCloseIoExceptions() {
    final AtomicBoolean closeCalled = new AtomicBoolean(false);
    final AtomicInteger sendAttempts = new AtomicInteger(0);
    HttpResponder failingResponder = new AbstractHttpResponder() {
      @Override
      public ChunkResponder sendChunkStart(HttpResponseStatus status, HttpHeaders headers) {
        return new ChunkResponder() {
          @Override
          public void sendChunk(ByteBuffer chunk) throws IOException {
            sendAttempts.incrementAndGet();
            throw new IOException("broken pipe");
          }

          @Override
          public void sendChunk(ByteBuf chunk) throws IOException {
            sendAttempts.incrementAndGet();
            throw new IOException("broken pipe");
          }

          @Override
          public void close() throws IOException {
            closeCalled.set(true);
            throw new IOException("responder close failed");
          }
        };
      }

      @Override
      public void sendContent(HttpResponseStatus status, ByteBuf content, HttpHeaders headers) {
      }

      @Override
      public void sendContent(HttpResponseStatus status, BodyProducer producer,
          HttpHeaders headers) {
      }

      @Override
      public void sendFile(File file, HttpHeaders headers) {
      }
    };

    TestChunkedCallback callback = new TestChunkedCallback(failingResponder);
    callback.init();
    callback.handle(createLogEvent("buffered"));
    callback.close();

    Assert.assertTrue(sendAttempts.get() > 0);
    Assert.assertTrue(closeCalled.get());
  }

  private static LogEvent createLogEvent(String message) {
    LoggingEvent event = new LoggingEvent();
    event.setLevel(Level.INFO);
    event.setMessage(message);
    event.setTimeStamp(1000L);
    return new LogEvent(event, new LogOffset(1L, 1000L));
  }

  private static final class TestChunkedCallback extends AbstractChunkedCallback {

    TestChunkedCallback(HttpResponder responder) {
      super(responder);
    }

    @Override
    protected HttpHeaders getResponseHeaders() {
      return new DefaultHttpHeaders().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
    }

    @Override
    protected void handleEvent(LogEvent event) {
      try {
        encodeSend(CharBuffer.wrap(event.getLoggingEvent().getFormattedMessage() + "\n"), false);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
