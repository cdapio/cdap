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
import co.cask.cdap.data.stream.service.StreamCoordinator;
import co.cask.cdap.data.stream.service.StreamMetaStore;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;

/**
 * In memory implementation for {@link StreamCoordinatorClient}.
 */
@Singleton
public final class InMemoryStreamCoordinatorClient
  extends AbstractStreamCoordinatorClient
  implements StreamCoordinator {

  private final StreamMetaStore streamMetaStore;
  private ListeningExecutorService executor;
  private final Set<StreamLeaderListener> leaderListeners;

  @Inject
  public InMemoryStreamCoordinatorClient(StreamAdmin streamAdmin, StreamMetaStore streamMetaStore) {
    super(streamAdmin);
    this.streamMetaStore = streamMetaStore;
    this.leaderListeners = Sets.newHashSet();
  }

  @Override
  protected void startUp() throws Exception {
    executor = MoreExecutors.listeningDecorator(
      Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("stream-leader-manager")));
    invokeLeaderListeners((String) null);
  }

  @Override
  protected void doShutDown() throws Exception {
    executor.shutdownNow();
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
  public ListenableFuture<Void> streamCreated(final String streamName) {
    // Note: the leader of a stream in local mode is always the only existing stream handler
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        invokeLeaderListeners(streamName);
        return null;
      }
    });
  }

  @Override
  public Cancellable addLeaderListener(final StreamLeaderListener listener) {
    // Create a wrapper around user's listener, to ensure that the cancelling behavior set in this method
    // is not overridden by user's code implementation of the equal method
    final StreamLeaderListener wrappedListener = new StreamLeaderListener() {
      @Override
      public void leaderOf(Set<String> streamNames) {
        listener.leaderOf(streamNames);
      }
    };

    synchronized (this) {
      leaderListeners.add(wrappedListener);
    }
    return new Cancellable() {
      @Override
      public void cancel() {
        synchronized (InMemoryStreamCoordinatorClient.this) {
          leaderListeners.remove(wrappedListener);
        }
      }
    };
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
