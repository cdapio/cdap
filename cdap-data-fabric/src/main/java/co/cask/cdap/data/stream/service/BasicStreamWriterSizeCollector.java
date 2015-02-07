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

import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data.stream.StreamPropertyListener;
import co.cask.cdap.proto.Id;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Basic implementation of a {@link StreamWriterSizeCollector}.
 */
public class BasicStreamWriterSizeCollector extends AbstractIdleService implements StreamWriterSizeCollector {
  private static final Logger LOG = LoggerFactory.getLogger(BasicStreamWriterSizeCollector.class);

  private final StreamCoordinatorClient streamCoordinatorClient;
  private final ConcurrentMap<Id.Stream, AtomicLong> streamSizes;
  private final List<Cancellable> truncationSubscriptions;

  @Inject
  public BasicStreamWriterSizeCollector(StreamCoordinatorClient streamCoordinatorClient) {
    this.streamCoordinatorClient = streamCoordinatorClient;
    this.streamSizes = Maps.newConcurrentMap();
    this.truncationSubscriptions = Lists.newArrayList();
  }

  @Override
  protected void startUp() throws Exception {
    // No-op
  }

  @Override
  protected void shutDown() throws Exception {
    for (Cancellable subscription : truncationSubscriptions) {
      subscription.cancel();
    }
  }

  @Override
  public long getTotalCollected(Id.Stream streamName) {
    AtomicLong collected = streamSizes.get(streamName);
    return collected != null ? collected.get() : 0;
  }

  @Override
  public synchronized void received(Id.Stream streamName, long dataSize) {
    AtomicLong value = streamSizes.get(streamName);
    if (value == null) {
      value = streamSizes.putIfAbsent(streamName, new AtomicLong(dataSize));
      if (value == null) {
        // This is the first time that we've seen this stream, we subscribe to generation changes to track truncation
        truncationSubscriptions.add(streamCoordinatorClient.addListener(streamName, new StreamPropertyListener() {
          @Override
          public void generationChanged(Id.Stream streamName, int generation) {
            // Handle stream truncation by resetting the size aggregated so far
            streamSizes.put(streamName, new AtomicLong(0));
          }
        }));
      }
    }
    if (value != null) {
      value.addAndGet(dataSize);
    }
    LOG.trace("Received data for stream {}: {}B. Total size is now {}", streamName, dataSize,
              value == null ? dataSize : value.get());
  }
}
