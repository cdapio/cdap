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

import java.util.Map;

/**
 * Heartbeat sent by a Stream writer containing the total size of its files, in bytes. The heartbeats
 * concerning one stream are aggregated by the {@link DistributedStreamService} elected leader of the stream.
 */
public class StreamWriterHeartbeat {

  private final long timestamp;
  private final int instanceId;
  private final Map<String, Long> streamsSizes;

  public StreamWriterHeartbeat(long timestamp, int instanceId, Map<String, Long> streamsSizes) {
    this.timestamp = timestamp;
    this.instanceId = instanceId;
    this.streamsSizes = ImmutableMap.copyOf(streamsSizes);
  }

  public long getTimestamp() {
    return timestamp;
  }

  public int getInstanceId() {
    return instanceId;
  }

  public Map<String, Long> getStreamsSizes() {
    return streamsSizes;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(StreamWriterHeartbeat.class)
      .add("timestamp", timestamp)
      .add("instanceId", instanceId)
      .add("streamsSizes", streamsSizes)
      .toString();
  }
}
