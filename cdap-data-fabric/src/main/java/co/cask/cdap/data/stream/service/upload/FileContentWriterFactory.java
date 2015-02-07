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
import co.cask.cdap.proto.Id;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.util.Map;

/**
 * A {@link ContentWriterFactory} that creates {@link ContentWriter} that will
 * write events to file directly.
 */
public final class FileContentWriterFactory implements ContentWriterFactory {

  private final StreamConfig streamConfig;
  private final ConcurrentStreamWriter streamWriter;
  private final Map<String, String> headers;
  private final Location streamTmpLocation;

  public FileContentWriterFactory(StreamConfig streamConfig,
                                  ConcurrentStreamWriter streamWriter, Map<String, String> headers) throws IOException {
    this.streamConfig = streamConfig;
    this.streamWriter = streamWriter;
    this.headers = ImmutableMap.copyOf(headers);

    Location location = streamConfig.getLocation();
    if (location == null) {
      // Should never happen
      throw new IllegalArgumentException("Unknown stream location for stream " + streamConfig.getName());
    }
    this.streamTmpLocation = location.append("tmp");
  }

  @Override
  public Id.Stream getStream() {
    return null;
    //TODO:
//    return streamConfig.getStreamId();
  }

  @Override
  public ContentWriter create(Map<String, String> headers) throws IOException {
    Map<String, String> allHeaders = Maps.newHashMap(this.headers);
    allHeaders.putAll(headers);
    Location uploadDir = streamTmpLocation.append("upload").getTempFile(Long.toString(System.currentTimeMillis()));
    uploadDir.mkdirs();
    return new FileContentWriter(streamConfig, streamWriter, uploadDir, allHeaders);
  }
}
