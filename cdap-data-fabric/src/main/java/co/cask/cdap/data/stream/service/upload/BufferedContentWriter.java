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

import co.cask.cdap.api.stream.StreamEventData;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.io.ByteBuffers;
import co.cask.cdap.data.stream.service.ConcurrentStreamWriter;
import co.cask.cdap.data.stream.service.MutableStreamEventData;
import co.cask.cdap.proto.Id;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A {@link ContentWriter} that buffer all events in memory and write to the actual stream writer when closed.
 */
final class BufferedContentWriter implements ContentWriter, Iterable<ByteBuffer> {

  private final Id.Stream streamId;
  private final ConcurrentStreamWriter streamWriter;
  private final Map<String, String> headers;
  private final List<ByteBuffer> bodies;

  BufferedContentWriter(Id.Stream streamId, ConcurrentStreamWriter streamWriter, Map<String, String> headers) {
    this.streamId = streamId;
    this.streamWriter = streamWriter;
    this.headers = ImmutableMap.copyOf(headers);
    this.bodies = Lists.newLinkedList();
  }

  @Override
  public void append(ByteBuffer body, boolean immutable) throws IOException {
    if (immutable) {
      bodies.add(body);
    } else {
      bodies.add(ByteBuffers.copy(body));
    }
  }

  @Override
  public void appendAll(Iterator<ByteBuffer> bodies, boolean immutable) throws IOException {
    while (bodies.hasNext()) {
      append(bodies.next(), immutable);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      streamWriter.enqueue(streamId, new StreamEventDataIterator(headers, bodies.iterator()));
    } catch (NotFoundException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void cancel() {
    // No-op
  }

  @Override
  public Iterator<ByteBuffer> iterator() {
    return bodies.iterator();
  }

  private static final class StreamEventDataIterator extends AbstractIterator<StreamEventData> {

    private final Iterator<? extends ByteBuffer> bodies;
    private final MutableStreamEventData streamEventData;

    private StreamEventDataIterator(Map<String, String> headers, Iterator<? extends ByteBuffer> bodies) {
      this.bodies = bodies;
      this.streamEventData = new MutableStreamEventData().setHeaders(headers);
    }

    @Override
    protected StreamEventData computeNext() {
      if (!bodies.hasNext()) {
        return endOfData();
      }
      return streamEventData.setBody(bodies.next());
    }
  }
}
