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

package co.cask.cdap.data.stream.service;

import co.cask.cdap.proto.id.StreamId;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Basic implementation of a {@link StreamWriterSizeCollector}.
 */
public class BasicStreamWriterSizeCollector implements StreamWriterSizeCollector {
  private static final Logger LOG = LoggerFactory.getLogger(BasicStreamWriterSizeCollector.class);

  private final ConcurrentMap<StreamId, AtomicLong> streamSizes;

  public BasicStreamWriterSizeCollector() {
    this.streamSizes = Maps.newConcurrentMap();
  }

  public Map<StreamId, AtomicLong> getStreamSizes() {
    return ImmutableMap.copyOf(streamSizes);
  }

  @Override
  public long getTotalCollected(StreamId streamId) {
    AtomicLong collected = streamSizes.get(streamId);
    return collected != null ? collected.get() : 0;
  }

  @Override
  public synchronized void received(StreamId streamId, long dataSize) {
    AtomicLong value = streamSizes.get(streamId);
    if (value == null) {
      value = streamSizes.putIfAbsent(streamId, new AtomicLong(dataSize));
    }
    if (value != null) {
      value.addAndGet(dataSize);
    }
    LOG.trace("Received data for stream {}: {}B. Total size is now {}", streamId, dataSize,
              value == null ? dataSize : value.get());
  }
}
