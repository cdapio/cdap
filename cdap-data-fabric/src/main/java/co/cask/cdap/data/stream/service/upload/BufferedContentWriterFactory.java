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
import co.cask.cdap.proto.Id;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;

/**
 * A {@link ContentWriterFactory} that creates {@link ContentWriter} that buffer all write requests in memory
 * and write them out at close time.
 */
public final class BufferedContentWriterFactory implements ContentWriterFactory {

  private final Id.Stream streamId;
  private final ConcurrentStreamWriter streamWriter;
  private final Map<String, String> headers;

  public BufferedContentWriterFactory(Id.Stream streamId, ConcurrentStreamWriter streamWriter,
                                      Map<String, String> headers) {
    this.streamId = streamId;
    this.streamWriter = streamWriter;
    this.headers = ImmutableMap.copyOf(headers);
  }

  @Override
  public Id.Stream getStream() {
    return streamId;
  }

  @Override
  public ContentWriter create(Map<String, String> headers) throws IOException {
    Map<String, String> allHeaders = Maps.newHashMap(this.headers);
    allHeaders.putAll(headers);
    return new BufferedContentWriter(streamId, streamWriter, allHeaders);
  }
}
