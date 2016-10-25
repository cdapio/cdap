/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.logging.gateway.handlers;

import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.logging.read.LogEvent;
import co.cask.http.BodyProducer;
import com.google.common.collect.Multimap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
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
  protected abstract Multimap<String, String> getResponseHeaders();

  protected abstract ChannelBuffer onWriteStart() throws IOException;
  protected abstract ChannelBuffer writeLogEvents(CloseableIterator<LogEvent> logEvent) throws IOException;
  protected abstract ChannelBuffer onWriteFinish() throws IOException;

  public void close() {
    logEventIter.close();
  }

  @Override
  public ChannelBuffer nextChunk() throws Exception {
    ChannelBuffer startBuffer = ChannelBuffers.EMPTY_BUFFER;
    if (!hasStarted) {
      hasStarted = true;
      startBuffer = ChannelBuffers.copiedBuffer(onWriteStart());
    }

    if (logEventIter.hasNext()) {
      ChannelBuffer eventsBuffer = writeLogEvents(logEventIter);
      return startBuffer.readable() ? ChannelBuffers.wrappedBuffer(startBuffer, eventsBuffer) : eventsBuffer;
    }

    if (!hasFinished) {
      hasFinished = true;
      return onWriteFinish();
    }

    return ChannelBuffers.EMPTY_BUFFER;
  }

  @Override
  public void finished() throws Exception {
    close();
  }

  @Override
  public void handleError(@Nullable Throwable throwable) {
    // previous behavior was to propagate the exception if its during sendChunk,
    // but suppress (and simply LOG.debug) if it was during close()
    // propagate it?
    LOG.error("Received error while chunking logs.", throwable);
    close();
  }

}
