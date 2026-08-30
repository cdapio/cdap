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
 * Provides static factory method to create {@link ZKClientService} with modified behaviors.
 */
public final class ZKClientServices {

  /**
   * Creates a {@link ZKClientService} from the given {@link ZKClient} if the given {@link ZKClient} is an instance of
   * {@link ZKClientService} or is a {@link ForwardingZKClient} that eventually trace back to a delegate of type
   * {@link ZKClientService}. If such a {@link ZKClientService} instance is found, this method returns
   * an instance by invoking {@link #delegate(ZKClient, ZKClientService)} with the given {@link ZKClient} and
   * the {@link ZKClientService} found respectively.
   *
   * @param client The {@link ZKClient}.
   * @return A {@link ZKClientService}.
   * @throws IllegalArgumentException If no {@link ZKClientService} is found.
   */
  public static ZKClientService delegate(ZKClient client) {
    ZKClient zkClient = client;
    while (!(zkClient instanceof ZKClientService) && zkClient instanceof ForwardingZKClient) {
      zkClient = ((ForwardingZKClient) zkClient).getDelegate();
    }
    if (zkClient instanceof ZKClientService) {
      return delegate(client, (ZKClientService) zkClient);
    }
    throw new IllegalArgumentException("No ZKClientService found from the delegation hierarchy");
  }

  /**
   * Creates a {@link ZKClientService} that for all {@link ZKClient} methods would be delegated to another
   * {@link ZKClient}, while methods for {@link ZKClientService} would be delegated to another {@link ZKClientService},
   * which the given {@link ZKClient} and {@link ZKClientService} could be different instances.
   *
   * @param client The {@link ZKClient} for delegation
   * @param clientService The {@link ZKClientService} for delegation.
   * @return A {@link ZKClientService}.
   */
  public static ZKClientService delegate(final ZKClient client, ZKClientService clientService) {
    return new ForwardingZKClientService(clientService) {

      @Override
      public Long getSessionId() {
        return client.getSessionId();
      }

      @Override
      public String getConnectString() {
        return client.getConnectString();
      }

      @Override
      public Cancellable addConnectionWatcher(Watcher watcher) {
        return client.addConnectionWatcher(watcher);
      }

      public OperationFuture<String> create(String path, @Nullable byte[] data,
                                            CreateMode createMode, boolean createParent, Iterable<ACL> acl) {
        return client.create(path, data, createMode, createParent, acl);
      }

      @Override
      public OperationFuture<Stat> exists(String path, @Nullable Watcher watcher) {
        return client.exists(path, watcher);
      }

      @Override
      public OperationFuture<NodeChildren> getChildren(String path, @Nullable Watcher watcher) {
        return client.getChildren(path, watcher);
      }

      @Override
      public OperationFuture<NodeData> getData(String path, @Nullable Watcher watcher) {
        return client.getData(path, watcher);
      }

      @Override
      public OperationFuture<Stat> setData(String dataPath, byte[] data, int version) {
        return client.setData(dataPath, data, version);
      }

      @Override
      public OperationFuture<String> delete(String deletePath, int version) {
        return client.delete(deletePath, version);
      }

      public OperationFuture<ACLData> getACL(String path) {
        return client.getACL(path);
      }

      public OperationFuture<Stat> setACL(String path, Iterable<ACL> acl, int version) {
        return client.setACL(path, acl, version);
      }
    };
  }

  private ZKClientServices() {
  }
}
