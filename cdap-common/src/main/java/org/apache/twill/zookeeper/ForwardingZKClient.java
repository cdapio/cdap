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
package org.apache.twill.zookeeper;

import org.apache.twill.common.Cancellable;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import javax.annotation.Nullable;

/**
 *
 */
public abstract class ForwardingZKClient extends AbstractZKClient {

  private final ZKClient delegate;

  protected ForwardingZKClient(ZKClient delegate) {
    this.delegate = delegate;
  }

  public final ZKClient getDelegate() {
    return delegate;
  }

  @Override
  public Long getSessionId() {
    return delegate.getSessionId();
  }

  @Override
  public String getConnectString() {
    return delegate.getConnectString();
  }

  @Override
  public Cancellable addConnectionWatcher(Watcher watcher) {
    return delegate.addConnectionWatcher(watcher);
  }

  @Override
  public OperationFuture<String> create(String path, @Nullable byte[] data,
                                        CreateMode createMode, boolean createParent, Iterable<ACL> acl) {
    return delegate.create(path, data, createMode, createParent, acl);
  }

  @Override
  public OperationFuture<Stat> exists(String path, @Nullable Watcher watcher) {
    return delegate.exists(path, watcher);
  }

  @Override
  public OperationFuture<NodeChildren> getChildren(String path, @Nullable Watcher watcher) {
    return delegate.getChildren(path, watcher);
  }

  @Override
  public OperationFuture<NodeData> getData(String path, @Nullable Watcher watcher) {
    return delegate.getData(path, watcher);
  }

  @Override
  public OperationFuture<Stat> setData(String dataPath, byte[] data, int version) {
    return delegate.setData(dataPath, data, version);
  }

  @Override
  public OperationFuture<String> delete(String deletePath, int version) {
    return delegate.delete(deletePath, version);
  }

  @Override
  public OperationFuture<ACLData> getACL(String path) {
    return delegate.getACL(path);
  }

  @Override
  public OperationFuture<Stat> setACL(String path, Iterable<ACL> acl, int version) {
    return delegate.setACL(path, acl, version);
  }
}
