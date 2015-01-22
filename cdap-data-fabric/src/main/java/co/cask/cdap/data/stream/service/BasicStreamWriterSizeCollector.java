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

import com.google.common.collect.Maps;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Basic implementation of a {@link StreamWriterSizeCollector}.
 */
public class BasicStreamWriterSizeCollector implements StreamWriterSizeCollector {

  private final ConcurrentMap<String, AtomicLong> streamSizes;

  public BasicStreamWriterSizeCollector() {
    this.streamSizes = Maps.newConcurrentMap();
  }

  @Override
  public long getTotalCollected(String streamName) {
    AtomicLong collected = streamSizes.get(streamName);
    return collected != null ? collected.get() : 0;
  }

  @Override
  public synchronized void received(String streamName, long dataSize) {
    AtomicLong value = streamSizes.get(streamName);
    if (value == null) {
      value = streamSizes.putIfAbsent(streamName, new AtomicLong(dataSize));
    }
    if (value != null) {
      value.addAndGet(dataSize);
    }
  }
}
