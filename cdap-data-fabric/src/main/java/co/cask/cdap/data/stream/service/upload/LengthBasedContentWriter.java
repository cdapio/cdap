/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data.stream.service.upload;

import co.cask.cdap.data.stream.service.ConcurrentStreamWriter;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

/**
 * Implementation of {@link ContentWriter} that dynamically decides to buffers all events in memory or writes to stream
 * file based on the length of streaming data.
 */
final class LengthBasedContentWriter implements ContentWriter {

  private final long bufferThreshold;
  private final BufferedContentWriter bufferedContentWriter;
  private final FileContentWriterFactory fileContentWriterFactory;

  private ContentWriter fileContentWriter;
  private long bodySize;

  LengthBasedContentWriter(StreamConfig streamConfig, ConcurrentStreamWriter streamWriter, Map<String, String> headers,
                           long bufferThreshold) throws IOException {
    this.bufferThreshold = bufferThreshold;
    this.bufferedContentWriter = (BufferedContentWriter) new BufferedContentWriterFactory(
      streamConfig.getStreamId(), streamWriter, headers).create(ImmutableMap.<String, String>of());
    this.fileContentWriterFactory = new FileContentWriterFactory(streamConfig, streamWriter, headers);
    bodySize = 0;
    fileContentWriter = null;
  }

  @Override
  public void append(ByteBuffer body, boolean immutable) throws IOException {
    if (fileContentWriter != null) {
      fileContentWriter.append(body, immutable);
    } else {
      int size = body.remaining();
      bufferedContentWriter.append(body, immutable);
      updateWriter(size);
    }
  }

  @Override
  public void appendAll(Iterator<ByteBuffer> bodies, boolean immutable) throws IOException {
    if (fileContentWriter != null) {
      fileContentWriter.appendAll(bodies, immutable);
    } else {
      while (bodies.hasNext()) {
        ByteBuffer next = bodies.next();
        int size = next.remaining();
        bufferedContentWriter.append(next, immutable);
        if (updateWriter(size)) {
          appendAll(bodies, immutable);
          break;
        }
      }
    }
  }

  @Override
  public void cancel() {
    if (fileContentWriter != null) {
      fileContentWriter.cancel();
    } else {
      bufferedContentWriter.cancel();
    }
  }

  @Override
  public void close() throws IOException {
    if (fileContentWriter != null) {
      fileContentWriter.close();
    } else {
      bufferedContentWriter.close();
    }
  }

  private boolean updateWriter(long length) throws IOException {
    bodySize += length;
    if (bodySize >= bufferThreshold) {
      fileContentWriter = fileContentWriterFactory.create(ImmutableMap.<String, String>of());
      fileContentWriter.appendAll(bufferedContentWriter.iterator(), true);
      bufferedContentWriter.cancel();
      return true;
    }
    return false;
  }
}
