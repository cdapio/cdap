/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.twill.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import javax.annotation.Nullable;

/**
 * An abstract base implementation of {@link ZKClient} that simplifies implementation by providing forwarding for
 * methods that are meant to be delegated to other methods.
 */
public abstract class AbstractZKClient implements ZKClient {

  @Override
  public final OperationFuture<String> create(String path, @Nullable byte[] data, CreateMode createMode) {
    return create(path, data, createMode, true);
  }

  @Override
  public final OperationFuture<String> create(String path, @Nullable byte[] data,
                                        CreateMode createMode, boolean createParent) {
    return create(path, data, createMode, createParent, ZooDefs.Ids.OPEN_ACL_UNSAFE);
  }

  @Override
  public final OperationFuture<String> create(String path, @Nullable byte[] data,
                                              CreateMode createMode, Iterable<ACL> acl) {
    return create(path, data, createMode, true, acl);
  }

  @Override
  public final OperationFuture<Stat> exists(String path) {
    return exists(path, null);
  }

  @Override
  public final OperationFuture<NodeChildren> getChildren(String path) {
    return getChildren(path, null);
  }

  @Override
  public final OperationFuture<NodeData> getData(String path) {
    return getData(path, null);
  }

  @Override
  public final OperationFuture<Stat> setData(String path, byte[] data) {
    return setData(path, data, -1);
  }

  @Override
  public final OperationFuture<String> delete(String path) {
    return delete(path, -1);
  }

  @Override
  public final OperationFuture<Stat> setACL(String path, Iterable<ACL> acl) {
    return setACL(path, acl, -1);
  }
}
