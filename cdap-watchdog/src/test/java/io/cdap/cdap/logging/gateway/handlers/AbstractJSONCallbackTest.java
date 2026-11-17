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
import com.google.common.base.Strings;
import io.cdap.cdap.logging.MockResponder;
import io.cdap.cdap.logging.read.LogEvent;
import io.cdap.cdap.logging.read.LogOffset;
import io.cdap.http.AbstractHttpResponder;
import io.cdap.http.BodyProducer;
import io.cdap.http.ChunkResponder;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link AbstractJSONCallback}.
 */
public class AbstractJSONCallbackTest {

  @Test
  public void testEmptyAndNonEmptyJsonArrayEncoding() {
    MockResponder emptyResponder = new MockResponder();
    TestJsonCallback emptyCallback = new TestJsonCallback(emptyResponder);
    emptyCallback.init();
    emptyCallback.close();
    Assert.assertEquals("[]", emptyResponder.getResponseContentAsString());

    MockResponder dataResponder = new MockResponder();
    TestJsonCallback dataCallback = new TestJsonCallback(dataResponder);
    dataCallback.init();
    dataCallback.handle(createLogEvent("first"));
    dataCallback.handle(createLogEvent("second"));
    dataCallback.close();
    Assert.assertEquals("[{\"msg\":\"first\"},{\"msg\":\"second\"}]",
        dataResponder.getResponseContentAsString());
  }

  @Test
  public void testCheckedIoExceptionWrappedInRuntimeException() {
    final IOException chunkWriteFailure = new IOException("socket closed during chunk flush");
    HttpResponder failingResponder = new AbstractHttpResponder() {
      @Override
      public ChunkResponder sendChunkStart(HttpResponseStatus status, HttpHeaders headers) {
        return new ChunkResponder() {
          @Override
          public void sendChunk(ByteBuffer chunk) throws IOException {
            throw chunkWriteFailure;
          }

          @Override
          public void sendChunk(ByteBuf chunk) throws IOException {
            throw chunkWriteFailure;
          }

          @Override
          public void close() {
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

    TestJsonCallback callback = new TestJsonCallback(failingResponder);
    callback.init();
    // Payload > 8192 bytes overflows chunkBuffer inside handleEvent and triggers sendChunk()
    LogEvent largeEvent = createLogEvent(Strings.repeat("x", 9000));

    try {
      callback.handle(largeEvent);
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException actual) {
      Assert.assertEquals(RuntimeException.class, actual.getClass());
      Assert.assertSame(chunkWriteFailure, actual.getCause());
    }
  }

  @Test
  public void testUncheckedRuntimeExceptionIsNotDoubleWrapped() {
    final IllegalArgumentException expectedError = new IllegalArgumentException("bad log event");
    MockResponder responder = new MockResponder();
    AbstractJSONCallback callback = new AbstractJSONCallback(responder) {
      @Override
      protected Object encodeSend(LogEvent logEvent) {
        throw expectedError;
      }
    };
    callback.init();

    try {
      callback.handle(createLogEvent("bad"));
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException actual) {
      Assert.assertSame(expectedError, actual);
    }
  }

  private static LogEvent createLogEvent(String message) {
    LoggingEvent event = new LoggingEvent();
    event.setLevel(Level.INFO);
    event.setMessage(message);
    event.setTimeStamp(2000L);
    return new LogEvent(event, new LogOffset(2L, 2000L));
  }

  private static final class TestJsonCallback extends AbstractJSONCallback {

    TestJsonCallback(HttpResponder responder) {
      super(responder);
    }

    @Override
    protected Object encodeSend(LogEvent logEvent) {
      return Collections.singletonMap("msg", logEvent.getLoggingEvent().getFormattedMessage());
    }
  }
}
