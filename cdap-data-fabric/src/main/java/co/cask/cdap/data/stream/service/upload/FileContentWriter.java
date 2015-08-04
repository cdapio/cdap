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

import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.data.stream.StreamDataFileConstants;
import co.cask.cdap.data.stream.StreamDataFileWriter;
import co.cask.cdap.data.stream.service.ConcurrentStreamWriter;
import co.cask.cdap.data.stream.service.MutableStreamEvent;
import co.cask.cdap.data.stream.service.MutableStreamEventData;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

/**
 * Implementation of {@link ContentWriter} that writes to stream file directly.
 */
final class FileContentWriter implements ContentWriter {

  private final StreamConfig streamConfig;
  private final ConcurrentStreamWriter streamWriter;
  private final MutableStreamEventData streamEventData;
  private final MutableStreamEvent streamEvent;
  private final Location eventFile;
  private final Location indexFile;
  private final StreamDataFileWriter writer;
  private long eventCount;

  FileContentWriter(StreamConfig streamConfig, ConcurrentStreamWriter streamWriter,
                    Location directory, Map<String, String> headers) throws IOException {
    this.streamConfig = streamConfig;
    this.streamWriter = streamWriter;
    this.streamEventData = new MutableStreamEventData();
    this.streamEvent = new MutableStreamEvent();

    directory.mkdirs();
    this.eventFile = directory.append("upload.dat");
    this.indexFile = directory.append("upload.idx");

    Map<String, String> properties = createStreamFileProperties(headers);
    properties.put(StreamDataFileConstants.Property.Key.UNI_TIMESTAMP,
                   StreamDataFileConstants.Property.Value.CLOSE_TIMESTAMP);
    this.writer = new StreamDataFileWriter(Locations.newOutputSupplier(eventFile),
                                           Locations.newOutputSupplier(indexFile),
                                           streamConfig.getIndexInterval(),
                                           properties);
  }

  private Map<String, String> createStreamFileProperties(Map<String, String> headers) {
    // Prepend "event." to each header key
    Map<String, String> properties = Maps.newHashMap();
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      properties.put(StreamDataFileConstants.Property.Key.EVENT_HEADER_PREFIX + entry.getKey(), entry.getValue());
    }
    return properties;
  }

  @Override
  public void append(ByteBuffer body, boolean immutable) throws IOException {
    doAppend(body, System.currentTimeMillis());
  }

  @Override
  public void appendAll(Iterator<ByteBuffer> bodies, boolean immutable) throws IOException {
    long timestamp = System.currentTimeMillis();
    while (bodies.hasNext()) {
      doAppend(bodies.next(), timestamp);
    }
  }

  @Override
  public void cancel() {
    Closeables.closeQuietly(writer);
    Locations.deleteQuietly(Locations.getParent(eventFile), true);
  }

  @Override
  public void close() throws IOException {
    try {
      writer.flush();
      streamWriter.appendFile(streamConfig.getStreamId(), eventFile, indexFile, eventCount, writer);
    } catch (NotFoundException e) {
      throw Throwables.propagate(e);
    } finally {
      Locations.deleteQuietly(Locations.getParent(eventFile), true);
    }
  }

  private void doAppend(ByteBuffer body, long timestamp) throws IOException {
    writer.append(streamEvent.set(streamEventData.setBody(body), timestamp));
    eventCount++;
  }
}
