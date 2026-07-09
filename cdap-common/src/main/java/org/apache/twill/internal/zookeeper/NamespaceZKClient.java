/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.internal.zookeeper;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.zookeeper.ACLData;
import org.apache.twill.zookeeper.ForwardingZKClient;
import org.apache.twill.zookeeper.NodeChildren;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.OperationFuture;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import javax.annotation.Nullable;

/**
 * A {@link ZKClient} that namespace every paths.
 */
public final class NamespaceZKClient extends ForwardingZKClient {
  // This class extends from ForwardingZKClient but overrides every method is for letting the
  // ZKClientServices delegate logic works.

  private final String namespace;
  private final ZKClient delegate;
  private final String connectString;

  public NamespaceZKClient(ZKClient delegate, String namespace) {
    super(delegate);
    this.namespace = namespace;
    this.delegate = delegate;
    this.connectString = delegate.getConnectString() + namespace;
  }

  @Override
  public Long getSessionId() {
    return delegate.getSessionId();
  }

  @Override
  public String getConnectString() {
    return connectString;
  }

  @Override
  public Cancellable addConnectionWatcher(Watcher watcher) {
    return delegate.addConnectionWatcher(watcher);
  }

  @Override
  public OperationFuture<String> create(String path, @Nullable byte[] data,
                                        CreateMode createMode, boolean createParent, Iterable<ACL> acl) {
    return relayPath(delegate.create(getNamespacedPath(path), data, createMode, createParent, acl),
                     this.<String>createFuture(path));
  }

  @Override
  public OperationFuture<Stat> exists(String path, @Nullable Watcher watcher) {
    return relayFuture(delegate.exists(getNamespacedPath(path), watcher), this.<Stat>createFuture(path));
  }

  @Override
  public OperationFuture<NodeChildren> getChildren(String path, @Nullable Watcher watcher) {
    return relayFuture(delegate.getChildren(getNamespacedPath(path), watcher), this.<NodeChildren>createFuture(path));
  }

  @Override
  public OperationFuture<NodeData> getData(String path, @Nullable Watcher watcher) {
    return relayFuture(delegate.getData(getNamespacedPath(path), watcher), this.<NodeData>createFuture(path));
  }

  @Override
  public OperationFuture<Stat> setData(String dataPath, byte[] data, int version) {
    return relayFuture(delegate.setData(getNamespacedPath(dataPath), data, version), this.<Stat>createFuture(dataPath));
  }

  @Override
  public OperationFuture<String> delete(String deletePath, int version) {
    return relayPath(delegate.delete(getNamespacedPath(deletePath), version), this.<String>createFuture(deletePath));
  }

  @Override
  public OperationFuture<ACLData> getACL(String path) {
    return relayFuture(delegate.getACL(getNamespacedPath(path)), this.<ACLData>createFuture(path));
  }

  @Override
  public OperationFuture<Stat> setACL(String path, Iterable<ACL> acl, int version) {
    return relayFuture(delegate.setACL(getNamespacedPath(path), acl, version), this.<Stat>createFuture(path));
  }

  /**
   * Returns the namespaced path for the given path. The returned path should be used when performing
   * ZK operations with the delegating ZKClient.
   */
  private String getNamespacedPath(String path) {
    if ("/".equals(path)) {
      return namespace;
    }
    return namespace + path;
  }

  private <V> SettableOperationFuture<V> createFuture(String path) {
    return SettableOperationFuture.create(path, Threads.SAME_THREAD_EXECUTOR);
  }

  private <V> OperationFuture<V> relayFuture(final OperationFuture<V> from, final SettableOperationFuture<V> to) {
    Futures.addCallback(from, new FutureCallback<V>() {
      @Override
      public void onSuccess(V result) {
        to.set(result);
      }

      @Override
      public void onFailure(Throwable t) {
        to.setException(t);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    return to;
  }

  private OperationFuture<String> relayPath(final OperationFuture<String> from,
                                            final SettableOperationFuture<String> to) {
    from.addListener(new Runnable() {
      @Override
      public void run() {
        try {
          String relativePath = from.get().substring(namespace.length());
          to.set(relativePath.isEmpty() ? "/" : relativePath);
        } catch (Exception e) {
          to.setException(e.getCause());
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    return to;
  }
}
