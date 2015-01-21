/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.util.Set;
import javax.annotation.Nullable;

/**
 * In memory implementation for {@link StreamCoordinatorClient}.
 */
@Singleton
public final class InMemoryStreamCoordinatorClient extends AbstractStreamCoordinatorClient {

  private final StreamMetaStore streamMetaStore;
  private final Set<StreamLeaderListener> leaderListeners;

  @Inject
  public InMemoryStreamCoordinatorClient(StreamAdmin streamAdmin, StreamMetaStore streamMetaStore) {
    super(streamAdmin);
    this.streamMetaStore = streamMetaStore;
    this.leaderListeners = Sets.newHashSet();
  }

  @Override
  protected void startUp() throws Exception {
    invokeLeaderListeners((String) null);
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
  public ListenableFuture<Void> streamCreated(String streamName) {
    try {
      invokeLeaderListeners(streamName);
    } catch (Exception e) {
      Throwables.propagate(e);
    }
    return Futures.immediateFuture(null);
  }

  private void invokeLeaderListeners(@Nullable String createdStream) throws Exception {
    Set<String> streamNames = Sets.newHashSet();
    for (StreamSpecification spec : streamMetaStore.listStreams()) {
      streamNames.add(spec.getName());
    }
    if (createdStream != null) {
      streamNames.add(createdStream);
    }
    invokeLeaderListeners(streamNames);
  }

  private void invokeLeaderListeners(Set<String> streamNames) {
    Set<StreamLeaderListener> callbacks;
    synchronized (this) {
      callbacks = ImmutableSet.copyOf(leaderListeners);
    }
    for (StreamLeaderListener callback : callbacks) {
      callback.leaderOf(streamNames);
    }
  }
}
