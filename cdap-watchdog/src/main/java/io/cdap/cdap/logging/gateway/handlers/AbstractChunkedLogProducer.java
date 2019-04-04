/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

import com.google.common.collect.Multimap;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.logging.read.LogEvent;
import io.cdap.http.BodyProducer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * LogReader BodyProducer class that delegates to subclasses for how to encode log events.
 */
public abstract class AbstractChunkedLogProducer extends BodyProducer {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractChunkedLogProducer.class);

  protected static final int BUFFER_BYTES = 8192;

  private final CloseableIterator<LogEvent> logEventIter;

  private boolean hasStarted = false;
  private boolean hasFinished = false;

  AbstractChunkedLogProducer(CloseableIterator<LogEvent> logEventIter) {
    this.logEventIter = logEventIter;
  }

  /**
   * Return {@link Multimap} of HTTP response headers
   */
  protected abstract HttpHeaders getResponseHeaders();

  protected abstract ByteBuf onWriteStart() throws IOException;
  protected abstract ByteBuf writeLogEvents(CloseableIterator<LogEvent> logEvent) throws IOException;
  protected abstract ByteBuf onWriteFinish() throws IOException;

  public void close() {
    logEventIter.close();
  }

  @Override
  public ByteBuf nextChunk() throws Exception {
    ByteBuf startBuffer = Unpooled.EMPTY_BUFFER;
    if (!hasStarted) {
      hasStarted = true;
      startBuffer = Unpooled.copiedBuffer(onWriteStart());
    }

    if (logEventIter.hasNext()) {
      ByteBuf eventsBuffer = writeLogEvents(logEventIter);
      return startBuffer.isReadable() ? Unpooled.wrappedBuffer(startBuffer, eventsBuffer) : eventsBuffer;
    }

    if (!hasFinished) {
      hasFinished = true;
      return Unpooled.wrappedBuffer(startBuffer, onWriteFinish());
    }

    return Unpooled.EMPTY_BUFFER;
  }

  @Override
  public void finished() throws Exception {
    close();
  }

  @Override
  public void handleError(@Nullable Throwable throwable) {
    // previous behavior was to propagate the exception if its during sendChunk,
    // but suppress (and simply LOG.debug) if it was during close()
    LOG.error("Received error while chunking logs.", throwable);
    close();
  }

}
