/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.data.stream;

import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.common.conf.InMemoryPropertyStore;
import co.cask.cdap.common.conf.PropertyStore;
import co.cask.cdap.common.io.Codec;
import co.cask.cdap.data.stream.service.StreamMetaStore;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.twill.discovery.Discoverable;

/**
 * In memory implementation for {@link StreamCoordinator}.
 */
@Singleton
public final class InMemoryStreamCoordinator extends AbstractStreamCoordinator {

  private final StreamMetaStore streamMetaStore;

  @Inject
  protected InMemoryStreamCoordinator(StreamAdmin streamAdmin, StreamMetaStore streamMetaStore) {
    super(streamAdmin);
    this.streamMetaStore = streamMetaStore;
  }

  @Override
  protected void startUp() throws Exception {
    for (StreamSpecification spec : streamMetaStore.listStreams()) {
      // TODO register streams to perform aggregation logic on them
    }
  }

  @Override
  protected void doShutDown() throws Exception {
    // No-op
  }

  @Override
  protected <T> PropertyStore<T> createPropertyStore(Codec<T> codec) {
    return new InMemoryPropertyStore<T>();
  }

  @Override
  public void setHandlerDiscoverable(Discoverable discoverable) {
    // No-op
  }

  @Override
  public ListenableFuture<Void> streamCreated(String streamName) {
    // TODO implement the aggregation logic of the one stream handler process here
    return Futures.immediateFuture(null);
  }
}
