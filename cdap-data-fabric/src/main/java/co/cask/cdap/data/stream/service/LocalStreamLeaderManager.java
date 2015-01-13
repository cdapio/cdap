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

import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.data.stream.service.heartbeat.StreamsHeartbeatsAggregator;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;

/**
 * Local implementation of the {@link StreamLeaderManager}. Only one Stream handler can exist in local mode, hence
 * every stream is affected the same leader: that one Stream handler.
 */
public class LocalStreamLeaderManager extends AbstractIdleService implements StreamLeaderManager {

  private final StreamMetaStore streamMetaStore;
  private final StreamsHeartbeatsAggregator streamsHeartbeatsAggregator;
  private ListeningExecutorService executor;

  @Inject
  public LocalStreamLeaderManager(StreamMetaStore streamMetaStore,
                                  StreamsHeartbeatsAggregator streamsHeartbeatsAggregator) {
    this.streamMetaStore = streamMetaStore;
    this.streamsHeartbeatsAggregator = streamsHeartbeatsAggregator;
  }

  @Override
  protected void startUp() throws Exception {
    streamsHeartbeatsAggregator.startAndWait();

    // Perform aggregation on all existing streams
    Collection<String> streamNames =
      Collections2.transform(streamMetaStore.listStreams(), new Function<StreamSpecification, String>() {
      @Nullable
      @Override
      public String apply(@Nullable StreamSpecification input) {
        return input != null ? input.getName() : null;
      }
    });
    streamsHeartbeatsAggregator.listenToStreams(streamNames);

    executor = MoreExecutors.listeningDecorator(
      Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("stream-leader-manager")));
  }

  @Override
  protected void shutDown() throws Exception {
    streamsHeartbeatsAggregator.stopAndWait();
  }

  @Override
  public void setHandlerDiscoverable(Discoverable discoverable) {
    // No-op
  }

  @Override
  public ListenableFuture<Void> affectLeader(final String streamName) {
    // Note: the leader of a stream in local mode is always the only existing stream handler
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        streamsHeartbeatsAggregator.listenToStream(streamName);
        return null;
      }
    });
  }
}
