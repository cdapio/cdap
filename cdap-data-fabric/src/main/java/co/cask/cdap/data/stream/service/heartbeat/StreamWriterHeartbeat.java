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

package co.cask.cdap.data.stream.service.heartbeat;

import co.cask.cdap.data.stream.service.DistributedStreamService;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Heartbeat sent by a Stream writer containing the total size of its files, in bytes. The heartbeats
 * concerning one stream are aggregated by the {@link DistributedStreamService} elected leader of the stream.
 */
public class StreamWriterHeartbeat {

  /**
   * Type of information that describes one {@link StreamSize} object.
   */
  public enum StreamSizeType {
    /**
     * Size sent during Stream writer initialization.
     */
    INIT,

    /**
     * Size sent during regular Stream writer life.
     */
    REGULAR
  }

  private final long timestamp;
  private final int writerID;
  private final Map<String, StreamSize> streamsSizes;

  private StreamWriterHeartbeat(long timestamp, int writerID, Map<String, StreamSize> streamsSizes) {
    this.timestamp = timestamp;
    this.writerID = writerID;
    this.streamsSizes = ImmutableMap.copyOf(streamsSizes);
  }

  public long getTimestamp() {
    return timestamp;
  }

  public int getWriterID() {
    return writerID;
  }

  public Map<String, StreamSize> getStreamsSizes() {
    return streamsSizes;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(StreamWriterHeartbeat.class)
      .add("timestamp", timestamp)
      .add("writerID", writerID)
      .add("streamsSizes", streamsSizes)
      .toString();
  }

  /**
   * Builder of a {@link StreamWriterHeartbeat}.
   */
  public static final class Builder {

    private long timestamp;
    private int writerID;
    private final Map<String, StreamSize> streamSizes;

    public Builder() {
      streamSizes = Maps.newHashMap();
    }

    public Builder setTimestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public Builder setWriterID(int writerID) {
      this.writerID = writerID;
      return this;
    }

    public Builder addStreamSize(String streamName, long absoluteDataSize, StreamSizeType streamSizeType) {
      streamSizes.put(streamName, new StreamSize(absoluteDataSize, streamSizeType));
      return this;
    }

    public StreamWriterHeartbeat build() {
      return new StreamWriterHeartbeat(timestamp, writerID, streamSizes);
    }
  }

  /**
   * POJO class describing a stream size for the current writer.
   */
  public static final class StreamSize {
    private final long absoluteDataSize;
    private final StreamSizeType streamSizeType;

    public StreamSize(long absoluteDataSize, StreamSizeType streamSizeType) {
      this.absoluteDataSize = absoluteDataSize;
      this.streamSizeType = streamSizeType;
    }

    public long getAbsoluteDataSize() {
      return absoluteDataSize;
    }

    public StreamSizeType getStreamSizeType() {
      return streamSizeType;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(StreamWriterHeartbeat.class)
        .add("absoluteDataSize", absoluteDataSize)
        .add("streamSizeType", streamSizeType)
        .toString();
    }
  }
}
