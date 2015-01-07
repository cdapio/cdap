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
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.apache.twill.discovery.Discoverable;

/**
 * Local implementation of the {@link StreamLeaderManager}. Only one Stream handler can exist in local mode, hence
 * every stream is affected the same leader: that one Stream handler.
 */
public class LocalStreamLeaderManager extends AbstractIdleService implements StreamLeaderManager {

  private final StreamMetaStore streamMetaStore;

  @Inject
  public LocalStreamLeaderManager(StreamMetaStore streamMetaStore) {
    this.streamMetaStore = streamMetaStore;
  }

  @Override
  protected void startUp() throws Exception {
    for (StreamSpecification spec : streamMetaStore.listStreams()) {
      // TODO register streams to perform aggregation logic on them
    }
  }

  @Override
  protected void shutDown() throws Exception {
    // No-op
  }

  @Override
  public void setHandlerDiscoverable(Discoverable discoverable) {
    // No-op
  }

  @Override
  public ListenableFuture<Void> affectLeader(String streamName) {
    // TODO implement the aggregation logic of the one stream handler process here
    return Futures.immediateFuture(null);
  }
}
