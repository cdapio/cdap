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

import com.google.common.base.Supplier;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Service;
import org.apache.twill.internal.zookeeper.DefaultZKClientService;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ZooKeeperServer;

/**
 * A {@link ZKClient} that extends from {@link Service} to provide lifecycle management functions.
 * The {@link #start()} method needed to be called before calling any other method on this interface.
 * When the client is no longer needed, call {@link #stop()} to release any resources that it holds.
 */
public interface ZKClientService extends ZKClient, Service {

  /**
   * Returns a {@link Supplier} of {@link ZooKeeper} that gives the current {@link ZooKeeper} in use at the moment
   * when {@link com.google.common.base.Supplier#get()} get called.
   *
   * @return A {@link Supplier Supplier&lt;ZooKeeper&gt;}
   */
  Supplier<ZooKeeper> getZooKeeperSupplier();

  /**
   * Builder for creating an implementation of {@link ZKClientService}.
   * The default client timeout is 10000ms.
   */
  final class Builder {

    private final String connectStr;
    private int timeout = ZooKeeperServer.DEFAULT_TICK_TIME * 20;
    private Watcher connectionWatcher;
    private Multimap<String, byte[]> auths = ArrayListMultimap.create();

    /**
     * Creates a {@link Builder} with the given ZooKeeper connection string.
     * @param connectStr The connection string.
     * @return A new instance of Builder.
     */
    public static Builder of(String connectStr) {
      return new Builder(connectStr);
    }

    /**
     * Sets the client timeout to the give milliseconds.
     * @param timeout timeout in milliseconds.
     * @return This builder
     */
    public Builder setSessionTimeout(int timeout) {
      this.timeout = timeout;
      return this;
    }

    /**
     * Sets a {@link Watcher} that will be called whenever connection state change.
     * @param watcher The watcher to set.
     * @return This builder.
     */
    public Builder setConnectionWatcher(Watcher watcher) {
      this.connectionWatcher = watcher;
      return this;
    }

    /**
     * Adds an authorization information.
     *
     * @param schema The authorization schema.
     * @param auth The authorization bytes.
     * @return This builder.
     */
    public Builder addAuthInfo(String schema, byte[] auth) {
      this.auths.put(schema, auth);
      return this;
    }

    /**
     * Creates an instance of {@link ZKClientService} with the settings of this builder.
     * @return A new instance of {@link ZKClientService}.
     */
    public ZKClientService build() {
      return new DefaultZKClientService(connectStr, timeout, connectionWatcher, auths);
    }

    private Builder(String connectStr) {
      this.connectStr = connectStr;
    }
  }
}
